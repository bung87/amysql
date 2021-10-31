when defined(ChronosAsync):
  import chronos/[asyncloop, asyncsync, handles, transport, timer]
  import nativesockets
  import chronos / transport
else:
  import asyncnet,asyncdispatch, times
import ./private/auth
import ./private/conn_auth
import ./private/protocol
import ./private/cap
import ./private/errors
import ./conn
import net  # needed for the SslContext type
import strutils
import asyncnet

import logging
import tables
import urlly

const MinEvictableIdleTime {.intdefine.} = 60_0000
const TimeBetweenEvictionRuns {.intdefine.} = 30_000

when defined(release):  setLogFilter(lvlInfo)

when defined(ssl):
  proc startTls(conn: Connection, ssl: SslContext): Future[void] {.async.} =
    # MySQL's equivalent of STARTTLS: we send a sort of stub response
    # here, do SSL setup, and continue afterwards with the encrypted connection
    if Cap.ssl notin conn.serverCaps:
      raise newException(ProtocolError, "Server does not support SSL")
    var buf: string = newStringOfCap(32)
    buf.setLen(4)
    var caps: set[Cap] = BasicClientCaps + {Cap.ssl}
    putU32(buf, cast[uint32](caps))
    putU32(buf, 65536'u32)  # max packet size, TODO: what should I put here?
    buf.add( char(Charset_utf8_ci) )
    # 23 bytes of filler
    for i in 1 .. 23:
      buf.add( char(0) )
    await conn.sendPacket(buf)
    # The server will respond with the SSL SERVER_HELLO packet.
    wrapConnectedSocket(ssl, conn.transp, handshake=SslHandshakeType.handshakeAsClient)
    # and, once the encryption is negotiated, we will continue
    # with the real handshake response.

proc roundtripQuery(conn:Connection, query: string):Future[void] {.async, tags:[IOEffect,RootEffect].} =
  # TODO if error occurs what should to do ?
  var buf: string = newStringOfCap(4 + 1 + len(query))
  buf.setLen(4)
  buf.add( char(Command.query) )
  buf.add(query)
  try:
    await conn.sendPacket(buf, resetSeqId=false)
    await conn.receivePacket()
  except Exception:
    discard
  if isERRPacket(conn):
    discard
    # raise parseErrorPacket(conn)

template addIdleCheck(conn: Connection) =
  const ValidationQuery = "SELECT 1"
  when TestWhileIdle:
    when not defined(ChronosAsync):
      let idleCheck = proc (fd:AsyncFD): bool  {.closure, gcsafe.} =
        if now() - conn.lastOperationTime >= initDuration(milliseconds=MinEvictableIdleTime):
          asyncCheck conn.roundtripQuery( ValidationQuery)
        return false
      addTimer(TimeBetweenEvictionRuns,oneshot=false,idleCheck)
    else:
      proc addInterval(every: Duration, cb: CallbackFunc, udata: pointer = nil): Future[void] =
        var retFuture = newFuture[void]("chronos.addInterval(Duration)")
        var interval:CallbackFunc
        proc scheduleNext(arg: pointer = nil) =
          if not retFuture.finished():
            addTimer(Moment.fromNow(every), interval,arg)

        interval = proc (arg: pointer = nil) {.gcsafe,raises: [Defect].} =
          cb(arg)
          scheduleNext(arg)

        scheduleNext(udata)
        return retFuture

      let idleCheck = proc (arg: pointer) {.gcsafe, raises: [Defect].} =
        if  Moment.now() - cast[Connection](arg).lastOperationTime  >= milliseconds(MinEvictableIdleTime):
          asyncSpawn cast[Connection](arg).roundtripQuery( ValidationQuery)
      
      asyncSpawn addInterval(milliseconds( TimeBetweenEvictionRuns),idleCheck,cast[pointer](conn))
    
proc finishEstablishingConnection(conn: Connection,
                                  username, password, database: string,
                                  handshakePacket: HandshakePacket): Future[void] {.async.} =
  # password authentication
  # https://dev.mysql.com/doc/internals/en/determining-authentication-method.html
  # In MySQL 5.7, the default authentication plugin is mysql_native_password.
  # As of MySQL 8.0, the default authentication plugin is changed to caching_sha2_password. 
  # https://dev.mysql.com/doc/refman/5.7/en/authentication-plugins.html
  # https://dev.mysql.com/doc/refman/8.0/en/authentication-plugins.html

  var authResponse = plugin_auth(handshakePacket.plugin, handshakePacket.scrambleBuff, password)

  await conn.writeHandshakeResponse(username, authResponse, database, handshakePacket.plugin)
  # await confirmation from the server
  await conn.receivePacket()
  conn.resetPacketLen
  if isOKPacket(conn):
    conn.authenticated = true
    conn.addIdleCheck()
    return
  elif isERRPacket(conn):
    raise parseErrorPacket(conn)
  elif isAuthSwitchRequestPacket(conn):
    debug "isAuthSwitchRequestPacket"
    let responseAuthSwitch = conn.parseAuthSwitchPacket()
    if Cap.pluginAuth in conn.serverCaps  and responseAuthSwitch.pluginName.len > 0:
      debug "plugin auth handshake:" & responseAuthSwitch.pluginName
      debug "pluginData:" & responseAuthSwitch.pluginData
      let authData = plugin_auth(responseAuthSwitch.pluginName,responseAuthSwitch.pluginData, password)
      var buf: string = newStringOfCap(32)
      buf.setLen(4)
      case responseAuthSwitch.pluginName
        of "mysql_old_password", "mysql_clear_password":
          putNulString(buf,authData)
        else:
          buf.add authData
      await conn.sendPacket(buf)
      await conn.receivePacket()
      if isOKPacket(conn):
        conn.authenticated = true
        conn.addIdleCheck()
        return
      elif isERRPacket(conn):
        raise parseErrorPacket(conn)
    else:
      debug "legacy handshake"
      var buf: string = newStringOfCap(32)
      buf.setLen(4)
      var data = scramble323(responseAuthSwitch.pluginData, password) # need to be zero terminated before send
      putNulString(buf,data)
      await conn.sendPacket(buf)
      await conn.receivePacket()
      if isOKPacket(conn):
        conn.authenticated = true
        conn.addIdleCheck()
        return
      elif isERRPacket(conn):
        raise parseErrorPacket(conn)
  elif isExtraAuthDataPacket(conn):
    debug "isExtraAuthDataPacket"
    # https://dev.mysql.com/doc/internals/en/successful-authentication.html
    if handshakePacket.plugin == "caching_sha2_password":
        await caching_sha2_password_auth(conn, password, handshakePacket.scrambleBuff)
    # elif handshakePacket.plugin == "sha256_password":
    #     discard await = sha256_password_auth(conn, auth_packet, password)
    else:
        raise newException(ProtocolError,"Received extra packet for auth method " & handshakePacket.plugin )
    conn.authenticated = true
    conn.addIdleCheck()
  else:
    raise newException(ProtocolError, "Unexpected packet received after sending client handshake")

proc connect(conn: Connection): Future[HandshakePacket] {.async.} =
  await conn.receivePacket()
  conn.resetPacketLen
  result = conn.parseHandshakePacket()

type Attrs = Table[string,string]

when declared(SslContext) and declared(startTls):
  proc establishConnection*(sock: AsyncSocket , username: string, password: string = "", database: string = "", connectAttrs = default(Attrs), ssl: SslContext): Future[Connection] {.async.} =
    result = Connection(socket: sock)
    result.connectAttrs = connectAttrs
    result.buf.setLen(MysqlBufSize)
    let handshakePacket = await connect(result)
    # Negotiate encryption
    await result.startTls(ssl)
    await result.finishEstablishingConnection(username, password, database, handshakePacket)

proc establishConnection*[T](sock: T, username: string, password: string = "", database: string = "", connectAttrs = default(Attrs)): Future[Connection] {.async.} =
  result = Connection(transp: sock)
  result.connectAttrs = connectAttrs
  result.buf.setLen(MysqlBufSize)
  let handshakePacket = await connect(result)
  await result.finishEstablishingConnection(username, password, database, handshakePacket)

proc parseTextRow(conn: Connection,columnCount: int): seq[string] =
  # duplicated
  result = newSeq[string]()
  var i = 0
  while i < columnCount:
    if conn.buf[conn.bufPos] == NullColumn:
      result.add("")
      incPos(conn)
    else:
      result.add(conn.buf.readLenStr(conn.bufPos))
    inc i

template fetchResultset(conn:typed, result:typed, onlyFirst:typed, isTextMode:static[bool], process:untyped): untyped {.dirty.} =
  # duplicated
  let columnCount = readLenInt(conn.buf, conn.bufPos)
  result.columns = await conn.receiveMetadata(columnCount)
  while true:
    await conn.receivePacket()
    conn.resetPacketLen
    if isEOFPacket(conn):
      result.status = parseEOFPacket(conn)
      break
    elif isTextMode and isOKPacket(conn):
      result.status = parseOKPacket(conn)
      break
    elif isERRPacket(conn):
      raise parseErrorPacket(conn)
    else:
      process
      when onlyFirst:
        continue

proc rawQuery(conn: Connection, query: string, onlyFirst:static[bool] = false): Future[ResultSet[string]] {.
               async,#[ tags: [ReadDbEffect, WriteDbEffect,RootEffect]]#.} =
  # duplicated
  await conn.sendQuery(query)
  await conn.receivePacket()
  conn.resetPacketLen
  if isOKPacket(conn):
    # Success, but no rows returned.
    result.status = parseOKPacket(conn)
  elif isERRPacket(conn):
    raise parseErrorPacket(conn)
  else:
    conn.fetchResultset( result, onlyFirst, true, result.rows.add(conn.parseTextRow(columnCount)))

proc handleParams(conn: Connection, q: seq[(string, string)]) {.async.} =
  ## SHOW VARIABLES;
  ## https://dev.mysql.com/doc/refman/8.0/en/using-system-variables.html
  var key, val: string
  var cmd = "SET "
  var pos = 0
  for (key, val) in q:
    case key
    of "charset":
      # https://dev.mysql.com/doc/refman/8.0/en/set-names.html
      let charsets = val.split(",")
      for charset in charsets:
        discard await conn.rawQuery("SET NAMES " & charset)
    of "connection-attributes":
      continue
    else:
      if pos != 0:
        cmd.add ','
      cmd.add key & '=' & val
      inc pos
  if cmd.len  > 4:
    discard await conn.rawQuery cmd

proc handleConnectAttrs(q: seq[(string, string)]): Table[string,string] =
  for (key, val) in q:
    case key
      of "connection-attributes":
        let pairs = val[1 ..< ^1]
        # e.g. attr1=val1,attr2,attr3=
        # a missing key value evaluates as an empty string.
        let attrs = pairs.split(",")
        for p in attrs:
          let kv = p.split("=")
          if len(kv) == 1:
            result[kv[0]] = ""
          else:
            result[kv[0]] = kv[1]

proc open*(uriStr: string | urlly.Url): Future[Connection] {.async.} =
  ## https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html
  let uri:urlly.Url = when uriStr is string: urlly.parseUrl(uriStr) else: uriStr
  let port = if uri.port.len > 0: parseInt(uri.port).int32 else: 3306'i32
  when not defined(ChronosAsync):
    let transp = newAsyncSocket(AF_INET, SOCK_STREAM, buffered = true)
    await connect(transp, uri.hostname, Port(port))
  else:
    let isIp = net.isIpAddress(uri.hostname)
    let host:string = if isIp: uri.hostname else: nativesockets.getHostByName(uri.hostname).addrList[^1]
    let transp = await connect(initTAddress(host,Port(port) ))
  let connectAttrs = handleConnectAttrs(uri.query)
  result = await establishConnection(transp, uri.username, uri.password, uri.path[ 1 .. uri.path.high ],connectAttrs )
  if uri.query.len > 0:
    await result.handleParams(uri.query)

proc open*(connection, user, password:string; database = ""; connectAttrs:Table[string,string] = default(Table[string, string])): Future[Connection] {.async, #[tags: [DbEffect]]#.} =
  var isPath = false
  when not defined(ChronosAsync):
    var sock:AsyncSocket
  else:
    var sock:StreamTransport
  when defined(posix):
    isPath = connection[0] == '/'
  if isPath:
    when not defined(ChronosAsync):
      sock = newAsyncSocket(AF_UNIX, SOCK_STREAM, buffered = true)
      await connectUnix(sock,connection)
    else:
      sock = await connect initTAddress(connection)
  else:
    let
      colonPos = connection.find(':')
      host = if colonPos < 0: connection
            else: substr(connection, 0, colonPos-1)
      port: int32 = if colonPos < 0: 3306'i32
                    else: substr(connection, colonPos+1).parseInt.int32
    when not defined(ChronosAsync):
      sock = newAsyncSocket(AF_INET, SOCK_STREAM, buffered = true)
      await connect(sock, host, Port(port))
    else:
      let isIp = net.isIpAddress(host)
      let ip:string = if isIp: host else: nativesockets.getHostByName(host).addrList[^1]
      sock = await connect( initTAddress(ip, Port(port)))
  result = await establishConnection(sock, user, password, database, connectAttrs)

proc close*(conn: Connection): Future[void] {.async, #[tags: [DbEffect]]#.} =
  var buf: string = newStringOfCap(5)
  buf.setLen(4)
  buf.add( char(Command.quit) )
  await conn.sendPacket(buf, resetSeqId=true)
  await conn.receivePacket(drop_ok=true)
  when defined(ChronosAsync):
    await conn.transp.closeWait()
  else:
    conn.transp.close()
