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
# include async_mysqlpkg/private/protocol_basic
include async_mysqlpkg/private/protocol
import asyncnet, asyncdispatch,std/sha1
import strutils#, unsigned
import openssl  # Needed for sha1 from libcrypto even if we don't support ssl connections

when defined(ssl):
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

  ColumnDefinition* = object {.final.}
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

  ResultSet*[T] = object {.final.}
    status*     : ResponseOK
    columns*    : seq[ColumnDefinition]
    rows*       : seq[seq[T]]

  PreparedStatement* = ref PreparedStatementObj
  PreparedStatementObj = object
    statement_id: array[4, char]
    parameters: seq[ColumnDefinition]
    columns: seq[ColumnDefinition]
    warnings: Natural

proc add(s: var string, a: seq[char]) =
  for ch in a:
    s.add(ch)



## ######################################################################
##
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
  # if isNil(s):
  #   ParameterBinding(typ: paramNull)
  # else:
  ParameterBinding(typ: paramString, strVal: s)

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

proc isNil*(v: ResultValue): bool {.inline.} = v.typ == rvtNull

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


when declared(openssl.EvpSHA1) and declared(EvpDigestCtxCreate):
  # This implements the "mysql_native_password" auth plugin,
  # which is the only auth we support.
  proc mysql_native_password_hash(scramble: string, password: string): string =
    let sha1 = EvpSHA1()
    let ctx = EvpDigestCtxCreate()
    proc add(buf: string) = ctx.update(cast[seq[char]](buf))
    proc add(buf: seq[uint8]) {.inline.} = ctx.update(cast[seq[char]](buf))
    proc hashfinal(): seq[char] =
      newSeq(result, EvpDigestSize(sha1))
      if ctx.final(result[0].addr, nil) == 0:
        doAssert(false, "EVP_DigestFinal_ex failed")
      ctx.cleanup()

    block:
      let ok = ctx.init(sha1, nil)
      doAssert(ok != 0, "EVP_DigestInit_ex failed")
    add(password)
    let phash1 = hashfinal()

    block:
      let ok = ctx.init(sha1, nil)
      doAssert(ok != 0, "EVP_DigestInit_ex failed")
    ctx.update(phash1)
    let phash2 = hashfinal()

    block:
      let ok = ctx.init(sha1, nil)
      doAssert(ok != 0, "EVP_DigestInit_ex failed")
    add(scramble)
    ctx.update(phash2)
    let rhs = hashfinal()

    EvpDigestCtxDestroy(ctx)

    result = newString(len(phash1))
    for i in 0 .. len(phash1)-1:
      result[i] = char(uint8(phash1[i]) xor uint8(rhs[i]))


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

proc `xor`(a: string, b: string): string =
  assert a.len == b.len
  result = newStringOfCap(a.len)
  for i in 0..<a.len:
    let c = ord(a[i]) xor ord(b[i])
    add(result, chr(c))

proc sha1(seed: string): string =
  const len = 20
  result = newString(len)
  let s = secureHash(seed)
  let da = Sha1Digest(s)
  for i in 0..<len:
    result[i] = chr(da[i])

proc token(scrambleBuff: string, password: string): string =
  let stage1 = sha1(password)
  let stage2 = sha1(stage1)
  let stage3 = sha1(scrambleBuff & stage2)
  result = stage3 xor stage1

proc finishEstablishingConnection(conn: Connection,
                                  username, password, database: string,
                                  greet: greetingVars): Future[void] {.async.} =
  # password authentication
  when declared(mysql_native_password_hash):
    let authResponse = (if isNil(password): nil else: mysql_native_password_hash(greet.scramble, password) )
  else:
    var authResponse = token(greet.scramble, password)
  await conn.writeHandshakeResponse(username, authResponse, database, "")

  # await confirmation from the server
  let pkt = await conn.receivePacket()
  if isOKPacket(pkt):
    return
  elif isERRPacket(pkt):
    raise parseErrorPacket(pkt)
  else:
    raise newException(ProtocolError, "Unexpected packet received after sending client handshake")

when declared(SslContext) and declared(startTls):
  proc establishConnection*(sock: AsyncSocket, username: string, password: string = "", database: string = "", ssl: SslContext): Future[Connection] {.async.} =
    result = Connection(socket: sock)
    let pkt = await result.receivePacket()
    let greet = result.parseInitialGreeting(pkt)

    # Negotiate encryption
    await result.startTls(ssl)
    await result.finishEstablishingConnection(username, password, database, greet)

proc establishConnection*(sock: AsyncSocket, username: string, password: string = "", database: string = ""): Future[Connection] {.async.} =
  result = Connection(socket: sock)
  let pkt = await result.receivePacket()
  let greet = result.parseInitialGreeting(pkt)
  await result.finishEstablishingConnection(username, password, database, greet)

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
  buf.add( char(Command.quiT) )
  await conn.sendPacket(buf, reset_seq_no=true)
  let pkt = await conn.receivePacket(drop_ok=true)
  conn.socket.close()

## ######################################################################
##
## Internal tests
## These don't try to test everything, just basic things and things
## that won't be exercised by functional testing against a server


when isMainModule or defined(test):
  proc hexstr(s: string): string =
    result = ""
    let chs = "0123456789abcdef"
    for ch in s:
      let i = int(ch)
      result.add(chs[ (i and 0xF0) shr 4])
      result.add(chs[  i and 0x0F ])
  proc expect(expected: string, got: string) =
    if expected == got:
      stdmsg.writeLine("OK")
    else:
      stdmsg.writeLine("FAIL")
      stdmsg.writeLine("    expected: ", expected)
      stdmsg.writeLine("         got: ", got)
  proc expectint[T](expected: T, got: T): int =
    if expected == got:
      return 0
    stdmsg.write(" ", expected, "!=", got)
    return 1
  when declared(openssl.EvpSHA1) and declared(EvpDigestCtxCreate):
    proc test_native_hash(scramble: string, password: string, expected: string) =
      let got = mysql_native_password_hash(scramble, password)
      expect(expected, hexstr(got))

    proc test_hashes() =
      echo "- Password hashing"
      # Test vectors captured from tcp traces of official mysql
      stdmsg.write("  test vec 1: ")
      test_native_hash("L\\i{NQ09k2W>p<yk/DK+",
                      "foo",
                      "f828cd1387160a4c920f6c109d37285d281f7c85")
      stdmsg.write("  test vec 2: ")
      test_native_hash("<G.N}OR-(~e^+VQtrao-",
                      "aaaaaaaaaaaaaaaaaaaabbbbbbbbbb",
                      "78797fae31fc733107e778ee36e124436761bddc")

  proc test_prim_values() =
    echo "- Packing/unpacking of primitive types"
    stdmsg.write("  packing: ")
    var buf: string = ""
    putLenInt(buf, 0)
    putLenInt(buf, 1)
    putLenInt(buf, 250)
    putLenInt(buf, 251)
    putLenInt(buf, 252)
    putLenInt(buf, 512)
    putLenInt(buf, 640)
    putLenInt(buf, 65535)
    putLenInt(buf, 65536)
    putLenInt(buf, 15715755)
    putU32(buf, uint32(65535))
    putU32(buf, uint32(65536))
    putU32(buf, 0x80C00AAA'u32)
    expect("0001fafcfb00fcfc00fc0002fc8002fcfffffd000001fdabcdefffff000000000100aa0ac080", hexstr(buf))
    stdmsg.write("  unpacking: ")
    var pos: int = 0
    var fails: int = 0
    fails += expectint(0      , scanLenInt(buf, pos))
    fails += expectint(1      , scanLenInt(buf, pos))
    fails += expectint(250    , scanLenInt(buf, pos))
    fails += expectint(251    , scanLenInt(buf, pos))
    fails += expectint(252    , scanLenInt(buf, pos))
    fails += expectint(512    , scanLenInt(buf, pos))
    fails += expectint(640    , scanLenInt(buf, pos))
    fails += expectint(0x0FFFF, scanLenInt(buf, pos))
    fails += expectint(0x10000, scanLenInt(buf, pos))
    fails += expectint(15715755, scanLenInt(buf, pos))
    fails += expectint(65535, int(scanU32(buf, pos)))
    fails += expectint(65535'u16, scanU16(buf, pos))
    fails += expectint(255'u16, scanU16(buf, pos+1))
    fails += expectint(0'u16, scanU16(buf, pos+2))
    pos += 4
    fails += expectint(65536, int(scanU32(buf, pos)))
    pos += 4
    fails += expectint(0x80C00AAA, int(scanU32(buf, pos)))
    pos += 4
    fails += expectint(0x80C00AAA00010000'u64, scanU64(buf, pos-8))
    fails += expectint(len(buf), pos)
    if fails == 0:
      stdmsg.writeLine(" OK")
    else:
      stdmsg.writeLine(" FAIL")

  proc test_param_pack() =
    echo "- Testing parameter packing"
    let dummy_param = ColumnDefinition()
    var sth: PreparedStatement
    new(sth)
    sth.statement_id = ['\0', '\xFF', '\xAA', '\x55' ]
    sth.parameters = @[dummy_param, dummy_param, dummy_param, dummy_param, dummy_param, dummy_param, dummy_param, dummy_param]
    stdmsg.write("  packing small numbers, 1: ")
    let buf = formatBoundParams(sth, [ asParam(0), asParam(1), asParam(127), asParam(128), asParam(255), asParam(256), asParam(-1), asParam(-127) ])
    expect("000000001700ffaa5500010000000001" &  # packet header
           "01800180018001800180028001000100" &  # wire type info
           "00017f80ff0001ff81",                 # packed values
           hexstr(buf))
    stdmsg.write("  packing numbers and NULLs: ")
    sth.parameters = sth.parameters & dummy_param
    let buf2 = formatBoundParams(sth, [ asParam(-128), asParam(-129), asParam(-255), asParam(nil), asParam(nil), asParam(-256), asParam(-257), asParam(-32768), asParam(nil)  ])
    expect("000000001700ffaa550001000000180101" &  # packet header
           "010002000200020002000200" &            # wire type info
           "807fff01ff00fffffe0080",               # packed values
           hexstr(buf2))

    stdmsg.write("  more values: ")
    let buf3 = formatBoundParams(sth, [ asParam("hello"), asParam(nil),
      asParam(0xFFFF), asParam(0xF1F2F3), asParam(0xFFFFFFFF), asParam(0xFFFFFFFFFF),
      asParam(-12885), asParam(-2160069290), asParam(low(int64) + 512) ])
    expect("000000001700ffaa550001000000020001" &  # packet header
           "fe000280038003800880020008000800"   &  # wire type info
           "0568656c6c6ffffff3f2f100ffffffffffffffffff000000abcd56f53f7fffffffff0002000000000080",
           hexstr(buf3))

  proc runInternalTests*() =
    echo "Running asyncmysql internal tests"
    test_prim_values()
    test_param_pack()
    when declared(openssl.EvpSHA1) and declared(EvpDigestCtxCreate):
      test_hashes()

  when isMainModule:
    runInternalTests()
