import ./private/auth
import ./private/conn_auth
import ./private/protocol
import ./private/cap
import ./private/errors
import ./conn
import net  # needed for the SslContext type
import uri
import strutils
import asyncdispatch
import asyncnet

import logging

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
    wrapConnectedSocket(ssl, conn.socket, handshake=SslHandshakeType.handshakeAsClient)
    # and, once the encryption is negotiated, we will continue
    # with the real handshake response.

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
  let pkt = await conn.receivePacket()
  if isOKPacket(pkt):
    conn.authenticated = true
    return
  elif isERRPacket(pkt):
    raise parseErrorPacket(pkt)
  elif isAuthSwitchRequestPacket(pkt):
    debug "isAuthSwitchRequestPacket"
    let responseAuthSwitch = conn.parseAuthSwitchPacket(pkt)
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
      let pkt = await conn.receivePacket()
      if isERRPacket(pkt):
        raise parseErrorPacket(pkt)
      conn.authenticated = true
      return
    else:
      debug "legacy handshake"
      var buf: string = newStringOfCap(32)
      buf.setLen(4)
      var data = scramble323(responseAuthSwitch.pluginData, password) # need to be zero terminated before send
      putNulString(buf,data)
      await conn.sendPacket(buf)
      discard await conn.receivePacket()
      conn.authenticated = true
  elif isExtraAuthDataPacket(pkt):
    debug "isExtraAuthDataPacket"
    # https://dev.mysql.com/doc/internals/en/successful-authentication.html
    if handshakePacket.plugin == "caching_sha2_password":
        discard await caching_sha2_password_auth(conn, pkt, password, handshakePacket.scrambleBuff)
    # elif handshakePacket.plugin == "sha256_password":
    #     discard await = sha256_password_auth(conn, auth_packet, password)
    else:
        raise newException(ProtocolError,"Received extra packet for auth method " & handshakePacket.plugin )
    conn.authenticated = true
  else:
    raise newException(ProtocolError, "Unexpected packet received after sending client handshake")

proc connect(conn: Connection): Future[HandshakePacket] {.async.} =
  let pkt = await conn.receivePacket()
  result = conn.parseHandshakePacket(pkt)

when declared(SslContext) and declared(startTls):
  proc establishConnection*(sock: AsyncSocket, username: string, password: string = "", database: string = "", ssl: SslContext): Future[Connection] {.async.} =
    result = Connection(socket: sock)
    let handshakePacket = await connect(result)
    # Negotiate encryption
    await result.startTls(ssl)
    await result.finishEstablishingConnection(username, password, database, handshakePacket)

proc establishConnection*(sock: AsyncSocket, username: string, password: string = "", database: string = ""): Future[Connection] {.async.} =
  result = Connection(socket: sock)
  let handshakePacket = await connect(result)
  await result.finishEstablishingConnection(username, password, database, handshakePacket)

proc parseTextRow(pkt: string): seq[string] =
  # duplicated
  var pos = 0
  result = newSeq[string]()
  while pos < len(pkt):
    if pkt[pos] == NullColumn:
      result.add("")
      inc(pos)
    else:
      result.add(pkt.readLenStr(pos))

template fetchResultset2(conn:typed, pkt:typed, result:typed, onlyFirst:typed, isTextMode:static[bool], process:untyped): untyped =
  # duplicated
  var p = 0
  let column_count = readLenInt(pkt, p)
  result.columns = await conn.receiveMetadata(column_count)
  while true:
    let pkt = await conn.receivePacket()
    if isEOFPacket(pkt):
      result.status = parseEOFPacket(pkt)
      break
    elif isTextMode and isOKPacket(pkt):
      result.status = parseOKPacket(conn, pkt)
      break
    elif isERRPacket(pkt):
      raise parseErrorPacket(pkt)
    else:
      process
      when onlyFirst:
        continue

proc rawQuery(conn: Connection, query: string, onlyFirst:static[bool] = false): Future[ResultSet[string]] {.
               async,#[ tags: [ReadDbEffect, WriteDbEffect,RootEffect]]#.} =
  # duplicated
  await conn.sendQuery(query)
  let pkt = await conn.receivePacket()
  if isOKPacket(pkt):
    # Success, but no rows returned.
    result.status = parseOKPacket(conn, pkt)
  elif isERRPacket(pkt):
    raise parseErrorPacket(pkt)
  else:
    conn.fetchResultset2(pkt, result, onlyFirst, true, result.rows.add(parseTextRow(pkt)))

proc handleParams(conn: Connection, q: string) {.async.} =
  ## SHOW VARIABLES;
  ## https://dev.mysql.com/doc/refman/8.0/en/using-system-variables.html
  var key, val: string
  var cmd = "SET "
  var pos = 0
  for item in split(q,"&"):
    (key, val) = item.split("=")
    case key
    of "charset":
      let charsets = val.split(",")
      for charset in charsets:
        try:
          discard await conn.rawQuery("SET NAMES " & charset)
        except:
          discard
    else:
      if pos != 0:
        cmd.add ','
      cmd.add key & '=' & val
      inc pos
  discard await conn.rawQuery cmd

proc open*(uriStr: string | Uri): Future[Connection] {.async.} =
  ## https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html
  let uri:Uri = when uriStr is string: parseUri(uriStr) else: uriStr
  let port = if uri.port.len > 0: parseInt(uri.port).int32 else: 3306'i32
  let sock = newAsyncSocket(AF_INET, SOCK_STREAM)
  await connect(sock, uri.hostname, Port(port))
  result = await establishConnection(sock, uri.username, uri.password, uri.path[ 1 .. uri.path.high ] )
  if uri.query.len > 0:
    await result.handleParams(uri.query)

proc open*(connection, user, password:string; database = ""): Future[Connection] {.async, #[tags: [DbEffect]]#.} =
  var isPath = false
  var sock:AsyncSocket
  when defined(posix):
    isPath = connection[0] == '/'
  if isPath:
    sock = newAsyncSocket(AF_UNIX, SOCK_STREAM)
    await connectUnix(sock,connection)
  else:
    let
      colonPos = connection.find(':')
      host = if colonPos < 0: connection
            else: substr(connection, 0, colonPos-1)
      port: int32 = if colonPos < 0: 3306'i32
                    else: substr(connection, colonPos+1).parseInt.int32
    sock = newAsyncSocket(AF_INET, SOCK_STREAM)
    await connect(sock, host, Port(port))
  return await establishConnection(sock, user, password, database)

proc close*(conn: Connection): Future[void] {.async, #[tags: [DbEffect]]#.} =
  var buf: string = newStringOfCap(5)
  buf.setLen(4)
  buf.add( char(Command.quit) )
  await conn.sendPacket(buf, resetSeqId=true)
  discard await conn.receivePacket(drop_ok=true)
  conn.socket.close()
