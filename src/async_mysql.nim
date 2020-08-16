##
## This module implements (a subset of) the MySQL/MariaDB client
## protocol based on asyncnet and asyncdispatch.
##
## No attempt is made to make this look like the C-language
## libmysql API.
##
## This is currently very experimental.
##
## Copyright (c) 2015 William Lewis
##

include async_mysql/private/protocol
import async_mysql/private/mysqlparser
import async_mysql/private/auth
import asyncnet, asyncdispatch, macros
import strutils#, unsigned
import net  # needed for the SslContext type


type
  # This represents a value returned from the server when using
  # the prepared statement / binary protocol. For convenience's sake
  # we combine multiple wire types into the nearest Nim type.
  ResultValueType = enum
    rvtNull,
    rvtInteger,
    rvtLong,
    rvtULong,
    rvtFloat32,
    rvtFloat64,
    rvtDate,
    rvtTime,
    rvtDateTime,
    rvtString,
    rvtBlob
  ResultValue* = object
    case typ: ResultValueType
      of rvtInteger:
        intVal: int
      of rvtLong:
        longVal: int64
      of rvtULong:
        uLongVal: uint64
      of rvtString, rvtBlob:
        strVal: string
      of rvtNull:
        discard
      of rvtFloat32, rvtFloat64:
        discard # TODO
      of rvtDate, rvtTime, rvtDateTime:
        discard # TODO

  ParamBindingType = enum
    paramNull,
    paramString,
    paramBlob,
    paramInt,
    paramUInt,
    # paramFloat, paramDouble,
    # paramLazyString, paramLazyBlob,
  ParameterBinding* = object
    ## This represents a value we're sending to the server as a parameter.
    ## Since parameters' types are always sent along with their values,
    ## we choose the wire type of integers based on the particular value
    ## we're sending each time.
    case typ: ParamBindingType
      of paramNull:
        discard
      of paramString, paramBlob:
        strVal: string# not nil
      of paramInt:
        intVal: int64
      of paramUInt:
        uintVal: uint64

type

  ColumnDefinition* {.final.} = object 
    catalog*     : string
    schema*      : string
    table*       : string
    orig_table*  : string
    name*        : string
    orig_name*   : string

    charset      : int16
    length*      : uint32
    column_type* : FieldType
    flags*       : set[FieldFlag]
    decimals*    : int

  ResultSet*[T] {.final.} = object 
    status*     : ResponseOK
    columns*    : seq[ColumnDefinition]
    rows*       : seq[seq[T]]

  PreparedStatement* = ref PreparedStatementObj
  PreparedStatementObj = object
    statement_id: array[4, char]
    parameters: seq[ColumnDefinition]
    columns: seq[ColumnDefinition]
    warnings: Natural


## Parameter and result packers/unpackers

proc addTypeUnlessNULL(p: ParameterBinding, pkt: var string) =
  case p.typ
  of paramNull:
    return
  of paramString:
    pkt.add(char(fieldTypeString))
    pkt.add(char(0))
  of paramBlob:
    pkt.add(char(fieldTypeBlob))
    pkt.add(char(0))
  of paramInt:
    if p.intVal >= 0:
      if p.intVal < 256'i64:
        pkt.add(char(fieldTypeTiny))
      elif p.intVal < 65536'i64:
        pkt.add(char(fieldTypeShort))
      elif p.intVal < (65536'i64 * 65536'i64):
        pkt.add(char(fieldTypeLong))
      else:
        pkt.add(char(fieldTypeLongLong))
      pkt.add(char(0x80))
    else:
      if p.intVal >= -128:
        pkt.add(char(fieldTypeTiny))
      elif p.intVal >= -32768:
        pkt.add(char(fieldTypeShort))
      else:
        pkt.add(char(fieldTypeLongLong))
      pkt.add(char(0))
  of paramUInt:
    if p.uintVal < (65536'u64 * 65536'u64):
      pkt.add(char(fieldTypeLong))
    else:
      pkt.add(char(fieldTypeLongLong))
    pkt.add(char(0x80))

proc addValueUnlessNULL(p: ParameterBinding, pkt: var string) =
  case p.typ
  of paramNull:
    return
  of paramString, paramBlob:
    putLenStr(pkt, p.strVal)
  of paramInt:
    if p.intVal >= 0:
      pkt.putU8(p.intVal and 0xFF)
      if p.intVal >= 256:
        pkt.putU8((p.intVal shr 8) and 0xFF)
        if p.intVal >= 65536:
          pkt.putU16( (p.intVal shr 16).uint16 and 0xFFFF'u16)
          if p.intVal >= (65536'i64 * 65536'i64):
            pkt.putU32(uint32(p.intVal shr 32))
    else:
      if p.intVal >= -128:
        pkt.putU8(uint8(p.intVal + 256))
      elif p.intVal >= -32768:
        pkt.putU16(uint16(p.intVal + 65536))
      else:
        pkt.putS64(p.intVal)
  of paramUInt:
    putU32(pkt, uint32(p.uintVal and 0xFFFFFFFF'u64))
    if p.uintVal >= 0xFFFFFFFF'u64:
      putU32(pkt, uint32(p.uintVal shr 32))

proc approximatePackedSize(p: ParameterBinding): int {.inline.} =
  case p.typ
  of paramNull:
    return 0
  of paramString, paramBlob:
    return 5 + len(p.strVal)
  of paramInt, paramUInt:
    return 4

proc asParam*(s: string): ParameterBinding =
  ParameterBinding(typ: paramString,strVal:s)

macro asParam*(s: untyped): untyped =
  doAssert s.kind == nnkNilLit
  nnkObjConstr.newTree(
      newIdentNode("ParameterBinding"),
      nnkExprColonExpr.newTree(
        newIdentNode("typ"),
        newIdentNode("paramNull")
      )
    )

proc asParam*(i: int): ParameterBinding = ParameterBinding(typ: paramInt, intVal: i)

proc asParam*(i: uint): ParameterBinding =
  if i > uint(high(int)):
    ParameterBinding(typ: paramUInt, uintVal: uint64(i))
  else:
    ParameterBinding(typ: paramInt, intVal: int64(i))

proc asParam*(i: int64): ParameterBinding =
  ParameterBinding(typ: paramInt, intVal: i)

proc asParam*(i: uint64): ParameterBinding =
  if i > uint64(high(int)):
    ParameterBinding(typ: paramUInt, uintVal: i)
  else:
    ParameterBinding(typ: paramInt, intVal: int64(i))

proc asParam*(b: bool): ParameterBinding = ParameterBinding(typ: paramInt, intVal: if b: 1 else: 0)

proc isNull*(v: ResultValue): bool {.inline.} = v.typ == rvtNull

proc `$`*(v: ResultValue): string =
  case v.typ
  of rvtNull:
    return "NULL"
  of rvtString, rvtBlob:
    return v.strVal
  of rvtInteger:
    return $(v.intVal)
  of rvtLong:
    return $(v.longVal)
  of rvtULong:
    return $(v.uLongVal)
  else:
    return "(unrepresentable!)"


{.push overflowChecks: on .}
proc toNumber[T](v: ResultValue): T {.inline.} =
  case v.typ
  of rvtInteger:
    return T(v.intVal)
  of rvtLong:
    return T(v.longVal)
  of rvtULong:
    return T(v.uLongVal)
  of rvtNull:
    raise newException(ValueError, "NULL value")
  else:
    raise newException(ValueError, "cannot convert " & $(v.typ) & " to integer")

converter asInt8*(v: ResultValue): int8 = return toNumber[int8](v)
converter asInt*(v: ResultValue): int = return toNumber[int](v)
converter asUInt*(v: ResultValue): uint = return toNumber[uint](v)
converter asInt64*(v: ResultValue): int64 = return toNumber[int64](v)
converter asUint64*(v: ResultValue): uint64 = return toNumber[uint64](v)
{. pop .}

converter asString*(v: ResultValue): string =
  case v.typ
  of rvtNull:
    return ""
  of rvtString, rvtBlob:
    return v.strVal
  else:
    raise newException(ValueError, "value is " & $(v.typ) & ", not string")

converter asBool*(v: ResultValue): bool =
  case v.typ
  of rvtInteger:
    return v.intVal != 0
  of rvtLong:
    return v.longVal != 0
  of rvtULong:
    return v.uLongVal != 0
  of rvtNull:
    raise newException(ValueError, "NULL value")
  else:
    raise newException(ValueError, "cannot convert " & $(v.typ) & " to boolean")

proc parseTextRow(pkt: string): seq[string] =
  var pos = 0
  result = newSeq[string]()
  while pos < len(pkt):
    if pkt[pos] == NullColumn:
      result.add("")
      inc(pos)
    else:
      result.add(pkt.scanLenStr(pos))

proc receiveMetadata(conn: Connection, count: Positive): Future[seq[ColumnDefinition]] {.async.}  =
  var received = 0
  result = newSeq[ColumnDefinition](count)
  while received < count:
    let pkt = await conn.receivePacket()
    # hexdump(pkt, stdmsg)
    if uint8(pkt[0]) == ResponseCode_ERR or uint8(pkt[0]) == ResponseCode_EOF:
      raise newException(ProtocolError, "TODO")
    var pos = 0
    result[received].catalog = scanLenStr(pkt, pos)
    result[received].schema = scanLenStr(pkt, pos)
    result[received].table = scanLenStr(pkt, pos)
    result[received].orig_table = scanLenStr(pkt, pos)
    result[received].name = scanLenStr(pkt, pos)
    result[received].orig_name = scanLenStr(pkt, pos)
    let extras_len = scanLenInt(pkt, pos)
    if extras_len < 10 or (pos+extras_len > len(pkt)):
      raise newException(ProtocolError, "truncated column packet")
    result[received].charset = int16(scanU16(pkt, pos))
    result[received].length = scanU32(pkt, pos+2)
    result[received].column_type = FieldType(uint8(pkt[pos+6]))
    result[received].flags = cast[set[FieldFlag]](scanU16(pkt, pos+7))
    result[received].decimals = int(pkt[pos+9])
    inc(received)
  let endPacket = await conn.receivePacket()
  if uint8(endPacket[0]) != ResponseCode_EOF:
    raise newException(ProtocolError, "Expected EOF after column defs, got something else")

proc prepareStatement*(conn: Connection, query: string): Future[PreparedStatement] {.async.} =
  var buf: string = newStringOfCap(4 + 1 + len(query))
  buf.setLen(4)
  buf.add( char(Command.statementPrepare) )
  buf.add(query)
  await conn.sendPacket(buf, reset_seq_no=true)
  let pkt = await conn.receivePacket()
  if isERRPacket(pkt):
    raise parseErrorPacket(pkt)
  if pkt[0] != char(ResponseCode_OK) or len(pkt) < 12:
    raise newException(ProtocolError, "Unexpected response to STMT_PREPARE (len=" & $(pkt.len) & ", first byte=0x" & toHex(int(pkt[0]), 2) & ")")
  let num_columns = scanU16(pkt, 5)
  let num_params = scanU16(pkt, 7)
  let num_warnings = scanU16(pkt, 10)

  new(result)
  result.warnings = num_warnings
  for b in 0 .. 3: result.statement_id[b] = pkt[1+b]
  if num_params > 0'u16:
    result.parameters = await conn.receiveMetadata(int(num_params))
  else:
    result.parameters = newSeq[ColumnDefinition](0)
  if num_columns > 0'u16:
    result.columns = await conn.receiveMetadata(int(num_columns))

proc prepStmtBuf(stmt: PreparedStatement, buf: var string, cmd: Command, cap: int = 9) =
  buf = newStringOfCap(cap)
  buf.setLen(9)
  buf[4] = char(cmd)
  for b in 0..3: buf[b+5] = stmt.statement_id[b]

proc closeStatement*(conn: Connection, stmt: PreparedStatement): Future[void] =
  var buf: string
  stmt.prepStmtBuf(buf, Command.statementClose)
  return conn.sendPacket(buf, reset_seq_no=true)

proc resetStatement*(conn: Connection, stmt: PreparedStatement): Future[void] =
  var buf: string
  stmt.prepStmtBuf(buf, Command.statementReset)
  return conn.sendPacket(buf, reset_seq_no=true)

proc formatBoundParams(stmt: PreparedStatement, params: openarray[ParameterBinding]): string =
  if len(params) != len(stmt.parameters):
    raise newException(ValueError, "Wrong number of parameters supplied to prepared statement (got " & $len(params) & ", statement expects " & $len(stmt.parameters) & ")")
  var approx = 14 + ( (params.len + 7) div 8 ) + (params.len * 2)
  for p in params:
    approx += p.approximatePackedSize()
  stmt.prepStmtBuf(result, Command.statementExecute, cap = approx)
  result.putU8(uint8(CursorType.noCursor))
  result.putU32(1) # "iteration-count" always 1
  if stmt.parameters.len == 0:
    return
  # Compute the null bitmap
  var ch = 0
  for p in 0 .. high(stmt.parameters):
    let bit = p mod 8
    if bit == 0 and p > 0:
      result.add(char(ch))
      ch = 0
    if params[p].typ == paramNull:
      ch = ch or ( 1 shl bit )
  result.add(char(ch))
  result.add(char(1)) # new-params-bound flag
  for p in params:
    p.addTypeUnlessNULL(result)
  for p in params:
    p.addValueUnlessNULL(result)

proc parseBinaryRow(columns: seq[ColumnDefinition], pkt: string): seq[ResultValue] =
  let column_count = columns.len
  let bitmap_len = (column_count + 9) div 8
  if len(pkt) < (1 + bitmap_len) or pkt[0] != char(0):
    raise newException(ProtocolError, "Truncated or incorrect binary result row")
  newSeq(result, column_count)
  var pos = 1 + bitmap_len
  for ix in 0 .. column_count-1:
    # First, check whether this column's bit is set in the null
    # bitmap. The bitmap is offset by 2, for no apparent reason.
    let bitmap_index = ix + 2
    let bitmap_entry = uint8(pkt[ 1 + (bitmap_index div 8) ])
    if (bitmap_entry and uint8(1 shl (bitmap_index mod 8))) != 0'u8:
      # This value is NULL
      result[ix] = ResultValue(typ: rvtNull)
    else:
      let typ = columns[ix].column_type
      let uns = FieldFlag.unsigned in columns[ix].flags
      case typ
      of fieldTypeNull:
        result[ix] = ResultValue(typ: rvtNull)
      of fieldTypeTiny:
        let v = pkt[pos]
        inc(pos)
        let ext = (if uns: int(cast[uint8](v)) else: int(cast[int8](v)))
        result[ix] = ResultValue(typ: rvtInteger, intVal: ext)
      of fieldTypeShort, fieldTypeYear:
        let v = int(scanU16(pkt, pos))
        inc(pos, 2)
        let ext = (if uns or (v <= 32767): v else: 65536 - v)
        result[ix] = ResultValue(typ: rvtInteger, intVal: ext)
      of fieldTypeInt24, fieldTypeLong:
        let v = scanU32(pkt, pos)
        inc(pos, 4)
        var ext: int
        if not uns and (typ == fieldTypeInt24) and v >= 8388608'u32:
          ext = 16777216 - int(v)
        elif not uns and (typ == fieldTypeLong):
          ext = int( cast[int32](v) ) # rely on 2's-complement reinterpretation here
        else:
          ext = int(v)
        result[ix] = ResultValue(typ: rvtInteger, intVal: ext)
      of fieldTypeLongLong:
        let v = scanU64(pkt, pos)
        inc(pos, 8)
        if uns:
          result[ix] = ResultValue(typ: rvtULong, uLongVal: v)
        else:
          result[ix] = ResultValue(typ: rvtLong, longVal: cast[int64](v))
      of fieldTypeFloat, fieldTypeDouble, fieldTypeTime, fieldTypeDate, fieldTypeDateTime, fieldTypeTimestamp:
        raise newException(Exception, "Not implemented, TODO")
      of fieldTypeTinyBlob, fieldTypeMediumBlob, fieldTypeLongBlob, fieldTypeBlob, fieldTypeBit:
        result[ix] = ResultValue(typ: rvtBlob, strVal: scanLenStr(pkt, pos))
      of fieldTypeVarchar, fieldTypeVarString, fieldTypeString, fieldTypeDecimal, fieldTypeNewDecimal:
        result[ix] = ResultValue(typ: rvtString, strVal: scanLenStr(pkt, pos))
      of fieldTypeEnum, fieldTypeSet, fieldTypeGeometry:
        raise newException(ProtocolError, "Unexpected field type " & $(typ) & " in resultset")

proc execStatement(conn: Connection, stmt: PreparedStatement, params: openarray[ParameterBinding]): Future[void] =
  var pkt = formatBoundParams(stmt, params)
  return conn.sendPacket(pkt, reset_seq_no=true)

when defined(ssl):
  proc startTls(conn: Connection, ssl: SslContext): Future[void] {.async.} =
    # MySQL's equivalent of STARTTLS: we send a sort of stub response
    # here, do SSL setup, and continue afterwards with the encrypted connection
    if Cap.ssl notin conn.server_caps:
      raise newException(ProtocolError, "Server does not support SSL")
    var buf: string = newStringOfCap(32)
    buf.setLen(4)
    var caps: set[Cap] = { Cap.longPassword, Cap.protocol41, Cap.secureConnection, Cap.ssl }
    putU32(buf, cast[uint32](caps))
    putU32(buf, 65536'u32)  # max packet size, TODO: what should I put here?
    buf.add( char(Charset_utf8_ci) )
    # 23 bytes of filler
    for i in 1 .. 23:
      buf.add( char(0) )
    await conn.sendPacket(buf)
    # The server will respond with the SSL SERVER_HELLO packet.
    wrapSocket(ssl, conn.socket, handshake=handshakeAsClient)
    # and, once the encryption is negotiated, we will continue
    # with the real handshake response.

proc roundtrip(conn:Connection, data: string): Future[string] {.async.} =
  var buf: string = newStringOfCap(32)
  buf.setLen(4)
  buf.add data
  await conn.sendPacket(buf)
  let pkt = await conn.receivePacket()
  if isERRPacket(pkt):
    raise parseErrorPacket(pkt)
  return pkt

proc caching_sha2_password_auth(conn:Connection, pkt, scrambleBuff, password: string): Future[string] {.async.} =
  # pkt 
  # 1 status 0x01
  # 2 auth_method_data (string.EOF) -- extra auth-data beyond the initial challenge
  if password.len == 0:
    return await conn.roundtrip("")
  var pkt = pkt
  if pkt.isAuthSwitchRequestPacket():
    let responseAuthSwitch = conn.parseAuthSwitchPacket(pkt)
    let authData = scramble_caching_sha2(responseAuthSwitch.pluginData, password)
    pkt = await conn.roundtrip(authData)
  if not pkt.isExtraAuthDataPacket:
    raise newException(ProtocolError,"caching sha2: Unknown packet for fast auth:" & pkt)
  
  # magic numbers:
  # 2 - request public key
  # 3 - fast auth succeeded
  # 4 - need full auth
  # var pos: int = 1
  let n = int(pkt[1])
  if n == 3:
    pkt = await conn.receivePacket()
    if isERRPacket(pkt):
      raise parseErrorPacket(pkt)
    return pkt
  if n != 4:
    raise newException(ProtocolError,"caching sha2: Unknown packet for fast auth:" & $n)
  # full path
  debugEcho "full path magic number:" & $n
  # raise newException(CatchableError, "Unimplemented")
  # if conn.secure # Sending plain password via secure connection (Localhost via UNIX socket or ssl)
  return await conn.roundtrip(password & char(0))
  # if not conn.server_public_key:
  #   pkt = await roundtrip(conn, "2") 
  #   if not isExtraAuthDataPacket(pkt):
  #     raise newException(ProtocolError,"caching sha2: Unknown packet for public key: "  & pkt)
  #   conn.server_public_key = pkt[1..pkt.high]
  # let data = sha2_rsa_encrypt(password, scrambleBuff, conn.server_public_key)
  # pkt = await roundtrip(conn, data)
  # return pkt

proc finishEstablishingConnection(conn: Connection,
                                  username, password, database: string,
                                  handshakePacket: HandshakePacket): Future[void] {.async.} =
  # password authentication
  # https://dev.mysql.com/doc/internals/en/determining-authentication-method.html
  # In MySQL 5.7, the default authentication plugin is mysql_native_password.
  # As of MySQL 8.0, the default authentication plugin is changed to caching_sha2_password. 
  # https://dev.mysql.com/doc/refman/5.7/en/authentication-plugins.html
  # https://dev.mysql.com/doc/refman/8.0/en/authentication-plugins.html
  debugEcho $handshakePacket
  var authResponse = plugin_auth(handshakePacket.plugin, handshakePacket.scrambleBuff, password)

  await conn.writeHandshakeResponse(username, authResponse, database, handshakePacket.plugin)

  # await confirmation from the server
  let pkt = await conn.receivePacket()
  if isOKPacket(pkt):
    return
  elif isERRPacket(pkt):
    raise parseErrorPacket(pkt)
  elif isAuthSwitchRequestPacket(pkt):
    debugEcho "isAuthSwitchRequestPacket"
    let responseAuthSwitch = conn.parseAuthSwitchPacket(pkt)
    if Cap.pluginAuth in conn.server_caps  and responseAuthSwitch.pluginName.len > 0:
      debugEcho "plugin auth handshake:" & responseAuthSwitch.pluginName
      debugEcho "pluginData:" & responseAuthSwitch.pluginData
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
      
      return
    else:
      debugEcho "legacy handshake"
      # send legacy handshake
      var buf: string = newStringOfCap(32)
      buf.setLen(4)
      var data = scramble323(responseAuthSwitch.pluginData, password) # need to be zero terminated before send
      putNulString(buf,data)
      await conn.sendPacket(buf)
      discard await conn.receivePacket()
  elif isExtraAuthDataPacket(pkt):
    debugEcho "isExtraAuthDataPacket"
    # https://dev.mysql.com/doc/internals/en/successful-authentication.html
    if handshakePacket.plugin == "caching_sha2_password":
        discard await caching_sha2_password_auth(conn, pkt, password, handshakePacket.scrambleBuff)
    # elif handshakePacket.plugin == "sha256_password":
    #     discard await = sha256_password_auth(conn, auth_packet, password)
    else:
        raise newException(ProtocolError,"Received extra packet for auth method " & handshakePacket.plugin )
  else:
    raise newException(ProtocolError, "Unexpected packet received after sending client handshake")

proc connect(conn: Connection): Future[HandshakePacket] {.async.} =
  let pkt = await conn.receivePacket()
  var parser = initPacketParser(PacketParserKind.ppkHandshake)
  loadBuffer(parser, pkt.cstring, pkt.len)
  let finished = parseHandshake(parser, result)
  assert finished == true
  conn.thread_id = result.threadId.uint32
  conn.server_version = result.serverVersion
  conn.server_caps = cast[set[Cap]](result.capabilities)

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

proc textQuery*(conn: Connection, query: string): Future[ResultSet[string]] {.async.} =
  await conn.sendQuery(query)
  let pkt = await conn.receivePacket()
  if isOKPacket(pkt):
    # Success, but no rows returned.
    result.status = parseOKPacket(conn, pkt)
    result.columns = @[]
    result.rows = @[]
  elif isERRPacket(pkt):
    # Some kind of failure.
    raise parseErrorPacket(pkt)
  else:
    var p = 0
    let column_count = scanLenInt(pkt, p)
    result.columns = await conn.receiveMetadata(column_count)
    var rows: seq[seq[string]]
    newSeq(rows, 0)
    while true:
      let pkt = await conn.receivePacket()
      if isEOFPacket(pkt):
        result.status = parseEOFPacket(pkt)
        break
      elif isOKPacket(pkt):
        result.status = parseOKPacket(conn, pkt)
        break
      elif isERRPacket(pkt):
        raise parseErrorPacket(pkt)
      else:
        rows.add(parseTextRow(pkt))
    result.rows = rows
  return

proc performPreparedQuery(conn: Connection, pstmt: PreparedStatement, st: Future[void]): Future[ResultSet[ResultValue]] {.async.} =
  await st
  let initialPacket = await conn.receivePacket()
  if isOKPacket(initialPacket):
    # Success, but no rows returned.
    result.status = parseOKPacket(conn, initialPacket)
    result.columns = @[]
    result.rows = @[]
  elif isERRPacket(initialPacket):
    # Some kind of failure.
    raise parseErrorPacket(initialPacket)
  else:
    var p = 0
    let column_count = scanLenInt(initialPacket, p)
    result.columns = await conn.receiveMetadata(column_count)
    var rows: seq[seq[ResultValue]]
    newSeq(rows, 0)
    while true:
      let pkt = await conn.receivePacket()
      # hexdump(pkt, stdmsg)
      if isEOFPacket(pkt):
        result.status = parseEOFPacket(pkt)
        break
      elif isERRPacket(pkt):
        raise parseErrorPacket(pkt)
      else:
        rows.add(parseBinaryRow(result.columns, pkt))
    result.rows = rows

proc preparedQuery*(conn: Connection, pstmt: PreparedStatement, params: varargs[ParameterBinding, asParam]): Future[ResultSet[ResultValue]] =
  var pkt = formatBoundParams(pstmt, params)
  var sent = conn.sendPacket(pkt, reset_seq_no=true)
  return performPreparedQuery(conn, pstmt, sent)

proc selectDatabase*(conn: Connection, database: string): Future[ResponseOK] {.async.} =
  var buf: string = newStringOfCap(4 + 1 + len(database))
  buf.setLen(4)
  buf.add( char(Command.initDb) )
  buf.add(database)
  await conn.sendPacket(buf, reset_seq_no=true)
  let pkt = await conn.receivePacket()
  if isERRPacket(pkt):
    raise parseErrorPacket(pkt)
  elif isOKPacket(pkt):
    return parseOKPacket(conn, pkt)
  else:
    raise newException(ProtocolError, "unexpected response to COM_INIT_DB")

proc close*(conn: Connection): Future[void] {.async.} =
  var buf: string = newStringOfCap(5)
  buf.setLen(4)
  buf.add( char(Command.quit) )
  await conn.sendPacket(buf, reset_seq_no=true)
  let pkt = await conn.receivePacket(drop_ok=true)
  conn.socket.close()

