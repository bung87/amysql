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
## Copyright (c) 2020 Bung

import amysql/private/protocol
import amysql/private/cap
import amysql/private/auth
import amysql/private/conn_auth
import amysql/conn
export conn

import amysql/private/json_sql_format
export json_sql_format

import amysql/private/my_geometry
export my_geometry

import amysql/private/quote
export quote

import amysql/private/errors
export errors

import asyncdispatch
import macros except floatVal
import net  # needed for the SslContext type
import db_common
import strutils
import asyncnet
import uri
import times
import json

type
  Row* = seq[string] 
  ResultValueType = enum
    rvtNull,
    rvtInteger,
    rvtLong,
    rvtULong,
    rvtFloat, # float32
    rvtDouble,
    rvtDate,
    rvtTime,
    rvtDateTime,
    rvtTimestamp,
    rvtString,
    rvtBlob,
    rvtJson,
    rvtGeometry
  Date* = object of DateTime
  ResultValue* = object
    case typ: ResultValueType
      of rvtInteger:
        intVal: int
      of rvtLong:
        longVal: int64
      of rvtULong:
        uLongVal: uint64
      of rvtString, rvtBlob,rvtJson,rvtGeometry:
        strVal: string
      of rvtNull:
        discard
      of rvtFloat:
        floatVal: float32
      of rvtDouble:
        doubleVal: float64
      of rvtTime:
        durVal: Duration
      of rvtDate, rvtDateTime,rvtTimestamp:
        # https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
        datetimeVal: DateTime
  ParamBindingType = enum
    paramNull,
    paramString,
    paramBlob,
    paramInt,
    paramUInt,
    paramFloat, 
    paramDouble,
    paramDate,
    paramTime,
    paramDateTime,
    paramTimestamp,
    paramJson,
    paramGeometry
    # paramLazyString, paramLazyBlob,
  SqlParam* = object
    ## This represents a value we're sending to the server as a parameter.
    ## Since parameters' types are always sent along with their values,
    ## we choose the wire type of integers based on the particular value
    ## we're sending each time.
    case typ: ParamBindingType
      of paramNull:
        discard
      of paramString, paramBlob, paramJson,paramGeometry:
        strVal: string# not nil
      of paramInt:
        intVal: int64
      of paramUInt:
        uintVal: uint64
      of paramFloat:
        floatVal: float32
      of paramDouble:
        doubleVal: float64
      of paramDate, paramDateTime,paramTimestamp:
        datetimeVal: DateTime
      of paramTime:
        durVal: Duration
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
  SqlPrepared* = ref SqlPreparedObj
  SqlPreparedObj = object
    statement_id: array[4, char]
    parameters: seq[ColumnDefinition]
    columns: seq[ColumnDefinition]
    warnings: Natural

## Parameter and result packers/unpackers

proc approximatePackedSize(p: SqlParam): int {.inline.} =
  ## approximate packed size for reducing reallocations
  case p.typ
  of paramNull: result = 0
  of paramString, paramBlob, paramJson, paramGeometry:
    result = 5 + len(p.strVal)
  of paramInt, paramUInt, paramFloat, paramDate, paramTimestamp: 
    result = 4
  of paramDouble:
    return 8
  of paramTime:
    let dp = toParts(p.durVal)
    let micro = dp[Microseconds]
    result = if micro == 0: 8 + 1 else: 12 + 1
  of paramDateTime:
    # case t.IsZero():
    # return 1
    if p.datetimeVal.nanosecond != 0:
      return 11 + 1
    elif p.datetimeVal.second != 0 or p.datetimeVal.minute != 0 or p.datetimeVal.hour != 0:
      return 7 + 1
    else:
      return 4 + 1

proc paramToField(p: SqlParam): FieldType =
  ## types that unsigned false and no special process
  case p.typ
  of paramFloat: result = fieldTypeFloat
  of paramDouble: result = fieldTypeDouble
  of paramDate: result = fieldTypeDate
  of paramDateTime: result = fieldTypeDateTime
  of paramTimestamp: result = fieldTypeTimestamp
  of paramTime: result = fieldTypeTime
  else: discard

proc addTypeUnlessNULL(p: SqlParam, pkt: var string,conn: Connection) =
  ## see https://dev.mysql.com/doc/internals/en/x-protocol-messages-messages.html
  ## Param type
  ## Param Unsigned flag
  ## isUnsigned = dbType == MySqlDbType.UByte || dbType == MySqlDbType.UInt16 ||
  ## dbType == MySqlDbType.UInt24 || dbType == MySqlDbType.UInt32 || dbType == MySqlDbType.UInt64;
  case p.typ
  of paramNull:
    return
  of paramString:
    pkt.writeTypeAndFlag(fieldTypeString)
  of paramBlob:
    pkt.writeTypeAndFlag(fieldTypeBlob)
  of paramJson:
    ## MYSQL_TYPE_JSON is not allowed as Item_param lacks a proper implementation for val_json.
    ## https://github.com/mysql/mysql-server/blob/124c7ab1d6f914637521fd4463a993aa73403513/sql/sql_prepare.cc#L636-L639
    pkt.writeTypeAndFlag(fieldTypeString) # fieldTypeJson
  of paramGeometry:
    # fieldTypeGeometry mysql works well, but 5.5.5-10.4.14-MariaDB-1:10.4.14+maria~xenial not supported ?
    pkt.writeTypeAndFlag(fieldTypeBlob)
  of paramInt:
    pkt.writeTypeAndFlag(p.intVal)
  of paramUInt:
    pkt.writeTypeAndFlag(p.uintVal)
  else:
    pkt.writeTypeAndFlag(paramToField(p))

proc addValueUnlessNULL(p: SqlParam, pkt: var string, conn: Connection) =
  ## https://dev.mysql.com/doc/internals/en/x-protocol-messages-messages.html
  case p.typ
  of paramNull:
    return
  of paramString, paramBlob, paramJson, paramGeometry:
    putLenStr(pkt, p.strVal)
  of paramInt:
    putValue(pkt, p.intVal)
  of paramUInt:
    putValue(pkt, p.uintVal)
  of paramFloat:
    putFloat(pkt, p.floatVal)
  of paramDouble:
    putDouble(pkt, p.doubleVal)
  of paramDate:
    putDate(pkt, p.datetimeVal)
  of paramDateTime, paramTimestamp:
    putDateTime(pkt, p.datetimeVal)
  of paramTime:
    putTime(pkt, p.durVal)

proc asParam*(s: string): SqlParam =
  SqlParam(typ: paramString,strVal:s)

macro asParam*(s: untyped): untyped =
  doAssert s.kind == nnkNilLit
  nnkObjConstr.newTree(
      newIdentNode("SqlParam"),
      nnkExprColonExpr.newTree(
        newIdentNode("typ"),
        newIdentNode("paramNull")
      )
    )

proc asParam*(i: int): SqlParam = SqlParam(typ: paramInt, intVal: i)

proc asParam*(i: uint): SqlParam =
  if i > uint(high(int)):
    SqlParam(typ: paramUInt, uintVal: uint64(i))
  else:
    SqlParam(typ: paramInt, intVal: int64(i))

proc asParam*(i: int64): SqlParam =
  SqlParam(typ: paramInt, intVal: i)

proc asParam*(i: uint64): SqlParam =
  if i > uint64(high(int)):
    SqlParam(typ: paramUInt, uintVal: i)
  else:
    SqlParam(typ: paramInt, intVal: int64(i))

proc asParam*(f: float32): SqlParam = SqlParam(typ: paramFloat, floatVal: f)
proc asParam*(f: float64): SqlParam = SqlParam(typ: paramDouble, doubleVal: f)

proc asParam*(d: DateTime): SqlParam = SqlParam(typ: paramDateTime, datetimeVal: d)
proc asParam*(d: Date): SqlParam = SqlParam(typ: paramDate, datetimeVal: d)
proc asParam*(d: Time): SqlParam = SqlParam(typ: paramTimestamp, datetimeVal: d.utc)
proc asParam*(d: Duration): SqlParam = SqlParam(typ: paramTime, durVal: d)

proc asParam*(d: JsonNode): SqlParam = 
  var r:string 
  toUgly(r,d)
  SqlParam(typ: paramJson, strVal: r)

proc asParam*(d: MyGeometry): SqlParam = SqlParam(typ: paramGeometry, strVal: d.data )

proc asParam*(b: bool): SqlParam = SqlParam(typ: paramInt, intVal: if b: 1 else: 0)

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
  of rvtFloat:
    return $v.floatVal
  of rvtDouble:
    return $v.doubleVal
  of rvtDateTime:
    return v.datetimeVal.format("yyyy-MM-dd hh:mm:ss")
  of rvtDate:
    return v.datetimeVal.format("yyyy-MM-dd")
  of rvtTimestamp:
    $v.datetimeVal.toTime.toUnix
  of rvtTime:
    let dp = toParts(v.durVal)
    let hours = dp[Days] * 24 + dp[Hours]
    let prefix = if v.durVal < DurationZero: "-" else: ""
    return prefix & "$1:$2:$3" % [ $hours, $dp[Minutes], $dp[Seconds]]
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
  of rvtFloat:
    return T(v.floatVal)
  of rvtDouble:
    return T(v.doubleVal)
  of rvtTimestamp:
    return T(v.datetimeVal.toTime.toUnix)
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

converter asFloat*(v: ResultValue): float32 = return toNumber[float32](v)
converter asDouble*(v: ResultValue): float64 = return toNumber[float64](v)

converter asString*(v: ResultValue): string =
  case v.typ
  of rvtNull:
    return ""
  of rvtString, rvtBlob:
    return v.strVal
  else:
    raise newException(ValueError, "Can't convert " & $(v.typ) & " to string")

converter asDateTime*(v: ResultValue): DateTime =
  case v.typ
  of rvtNull:
    return DateTime()
  of rvtDateTime:
    return v.datetimeVal
  of rvtDate:
    return v.datetimeVal
  else:
    raise newException(ValueError, "Can't convert " & $(v.typ) & " to DateTime")

converter asDate*(v: ResultValue): Date =
  cast[Date](v.datetimeVal)

converter asTime*(v: ResultValue): Time =
  case v.typ
  of rvtTimestamp:
    v.datetimeVal.toTime
  else:
    raise newException(ValueError, "Can't convert " & $(v.typ) & " to Time")

converter asDuration*(v: ResultValue): Duration =
  case v.typ
  of rvtTime:
    v.durVal
  else:
    raise newException(ValueError, "Can't convert " & $(v.typ) & " to Duration")

converter asMyGeometry*(v: ResultValue): MyGeometry = 
  case v.typ
  of rvtGeometry:
    newMyGeometry(v.strVal)
  else:
    raise newException(ValueError, "Can't convert " & $(v.typ) & " to MyGeometry")

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
    raise newException(ValueError, "Can't convert " & $(v.typ) & " to boolean")

converter asJson*(v: ResultValue): JsonNode =
  case v.typ
  of rvtJson, rvtBlob:
    ## linux mariadb may store as blob (eg. mariadb version 10.4)
    parseJson v.strVal
  else:
    raise newException(ValueError, "Can't convert " & $(v.typ) & " to JsonNode")

proc initDate*(monthday: MonthdayRange, month: Month, year: int, zone: Timezone = local()): Date =
  var dt = initDateTime(monthday,month,year,0,0,0,zone)
  copyMem(result.addr,dt.addr,sizeof(Date))

proc parseTextRow(pkt: string): seq[string] =
  var pos = 0
  result = newSeq[string]()
  while pos < len(pkt):
    if pkt[pos] == NullColumn:
      result.add("")
      inc(pos)
    else:
      result.add(pkt.readLenStr(pos))

proc receiveMetadata(conn: Connection, count: Positive): Future[seq[ColumnDefinition]] {.async.}  =
  var received = 0
  result = newSeq[ColumnDefinition](count)
  while received < count:
    let pkt = await conn.receivePacket()
    if uint8(pkt[0]) == ResponseCode_ERR or uint8(pkt[0]) == ResponseCode_EOF:
      raise newException(ProtocolError, "TODO")
    var pos = 0
    result[received].catalog = readLenStr(pkt, pos)
    result[received].schema = readLenStr(pkt, pos)
    result[received].table = readLenStr(pkt, pos)
    result[received].orig_table = readLenStr(pkt, pos)
    result[received].name = readLenStr(pkt, pos)
    result[received].orig_name = readLenStr(pkt, pos)
    let extras_len = readLenInt(pkt, pos)
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

proc prepare*(conn: Connection, query: string): Future[SqlPrepared] {.async.} =
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

proc prepare(pstmt: SqlPrepared, buf: var string, cmd: Command, cap: int = 9) =
  buf = newStringOfCap(cap)
  buf.setLen(9)
  buf[4] = char(cmd)
  for b in 0..3: buf[b+5] = pstmt.statement_id[b]

proc finalize*(conn: Connection, pstmt: SqlPrepared): Future[void] =
  var buf: string
  pstmt.prepare(buf, Command.statementClose)
  return conn.sendPacket(buf, reset_seq_no=true)

proc reset*(conn: Connection, pstmt: SqlPrepared): Future[void] =
  var buf: string
  pstmt.prepare(buf, Command.statementReset)
  return conn.sendPacket(buf, reset_seq_no=true)

proc formatBoundParams(conn: Connection, pstmt: SqlPrepared, params: openarray[SqlParam]): string =
  ## see https://mariadb.com/kb/en/com_stmt_execute/
  if len(params) != len(pstmt.parameters):
    raise newException(ValueError, "Wrong number of parameters supplied to prepared statement (got " & $len(params) & ", statement expects " & $len(pstmt.parameters) & ")")
  var approx = 14 + ( (params.len + 7) div 8 ) + (params.len * 2)
  for p in params:
    approx += p.approximatePackedSize()
  pstmt.prepare(result, Command.statementExecute, cap = approx)
  result.putU8(uint8(CursorType.noCursor))
  result.putU32(1) # "iteration-count" always 1
  if pstmt.parameters.len == 0:
    return
  # Compute the null bitmap
  var ch = 0
  for p in 0 .. high(pstmt.parameters):
    let bit = p mod 8
    if bit == 0 and p > 0:
      result.add(char(ch))
      ch = 0
    if params[p].typ == paramNull:
      ch = ch or ( 1 shl bit )
  result.add(char(ch))
  result.add(char(1)) # new-params-bound flag
  for p in params:
    p.addTypeUnlessNULL(result, conn)
  for p in params:
    p.addValueUnlessNULL(result, conn)

proc parseBinaryRow(columns: seq[ColumnDefinition], pkt: string): seq[ResultValue] =
  ## see https://mariadb.com/kb/en/resultset-row/
  ## https://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html
  
  # For the Binary Protocol Resultset Row the num-fields and the field-pos need to add a offset of 2.
  # For COM_STMT_EXECUTE this offset is 0.
  const offset = 2 
  let column_count = columns.len
  let bitmapBytes = (column_count + offset + 7) div 8
  if len(pkt) < (1 + bitmapBytes) or pkt[0] != char(0):
    raise newException(ProtocolError, "Truncated or incorrect binary result row")
  newSeq(result, column_count)
  var pos = 1 + bitmapBytes
  for ix in 0 .. column_count-1:
    # First, check whether this column's bit is set in the null
    # bitmap.
    # https://dev.mysql.com/doc/internals/en/null-bitmap.html
    let fieldIndex = ix + offset
    let bytePos = fieldIndex div 8
    let bitPos = fieldIndex mod 8
    let bitmap_entry = uint8(pkt[ 1 + bytePos ])
    if (bitmap_entry and uint8(1 shl bitPos)) != 0'u8:
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
      of fieldTypeShort,fieldTypeYear:
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
      of fieldTypeFloat:
        let v = scanFloat(pkt,pos)
        inc(pos, 4)
        result[ix] = ResultValue(typ: rvtFloat, floatVal: v)
      of fieldTypeDouble:
        let v = scanDouble(pkt,pos)
        inc(pos, 8)
        result[ix] = ResultValue(typ: rvtDouble, doubleVal: v)
      of fieldTypeDateTime:
        result[ix] = ResultValue(typ: rvtDateTime, datetimeVal: readDateTime(pkt, pos))
      of fieldTypeDate:
        let year = int(pkt[pos+1]) + int(pkt[pos+2]) * 256
        inc(pos,2)
        let month = int(pkt[pos + 1])
        let day = int(pkt[pos + 2])
        inc(pos,2)
        let dt = initDate(day,month.Month,year.int)
        result[ix] = ResultValue(typ: rvtDate, datetimeVal: dt)
      of fieldTypeTimestamp:
        result[ix] = ResultValue(typ: rvtTimestamp, datetimeVal: readDateTime(pkt, pos))  
      of fieldTypeTime:
        result[ix] = ResultValue(typ: rvtTime, durVal: readTime(pkt, pos) )
      of fieldTypeTinyBlob, fieldTypeMediumBlob, fieldTypeLongBlob, fieldTypeBlob, fieldTypeBit:
        result[ix] = ResultValue(typ: rvtBlob, strVal: readLenStr(pkt, pos))
      of fieldTypeVarchar, fieldTypeVarString, fieldTypeString, fieldTypeDecimal, fieldTypeNewDecimal:
        result[ix] = ResultValue(typ: rvtString, strVal: readLenStr(pkt, pos))
      of fieldTypeJson:
        result[ix] = ResultValue(typ: rvtJson, strVal: readLenStr(pkt, pos))
      of fieldTypeGeometry:
        result[ix] = ResultValue(typ: rvtGeometry, strVal: readLenStr(pkt, pos))
      of fieldTypeEnum, fieldTypeSet:
        raise newException(ProtocolError, "Unexpected field type " & $(typ) & " in resultset")

proc query*(conn: Connection, pstmt: SqlPrepared, params: openarray[static[SqlParam]]): Future[void] =
  var pkt = conn.formatBoundParams(pstmt, params)
  return conn.sendPacket(pkt, reset_seq_no=true)

when defined(ssl):
  proc startTls(conn: Connection, ssl: SslContext): Future[void] {.async.} =
    # MySQL's equivalent of STARTTLS: we send a sort of stub response
    # here, do SSL setup, and continue afterwards with the encrypted connection
    if Cap.ssl notin conn.serverCaps:
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
    return
  elif isERRPacket(pkt):
    raise parseErrorPacket(pkt)
  elif isAuthSwitchRequestPacket(pkt):
    debugEcho "isAuthSwitchRequestPacket"
    let responseAuthSwitch = conn.parseAuthSwitchPacket(pkt)
    if Cap.pluginAuth in conn.serverCaps  and responseAuthSwitch.pluginName.len > 0:
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

{.push warning[ObservableStores]: off.}
proc rawQuery*(conn: Connection, query: string, onlyFirst = false): Future[ResultSet[string]] {.
               async, tags: [ReadDbEffect, WriteDbEffect,RootEffect].} =
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
    let column_count = readLenInt(pkt, p)
    result.columns = await conn.receiveMetadata(column_count)
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
        result.rows.add(parseTextRow(pkt))
        if onlyFirst:
          break
  return

proc performPreparedQuery(conn: Connection, pstmt: SqlPrepared, st: Future[void], onlyFirst = false): Future[ResultSet[ResultValue]] {.
                          async, tags:[RootEffect].} =
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
    let column_count = readLenInt(initialPacket, p)
    result.columns = await conn.receiveMetadata(column_count)
    while true:
      let pkt = await conn.receivePacket()
      if isEOFPacket(pkt):
        result.status = parseEOFPacket(pkt)
        break
      elif isERRPacket(pkt):
        raise parseErrorPacket(pkt)
      else:
        result.rows.add(parseBinaryRow(result.columns, pkt))
        if onlyFirst:
          break
{.pop.}

proc query*(conn: Connection, pstmt: SqlPrepared, params: varargs[SqlParam, asParam]): Future[ResultSet[ResultValue]] {.
            #[tags: [ReadDbEffect, WriteDbEffect]]#.} =
  var pkt = conn.formatBoundParams(pstmt, params)
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

proc dbFormat(formatstr: SqlQuery, args: varargs[string]): string =
  result = ""
  var a = 0
  for c in items(string(formatstr)):
    if c == '?':
      add(result, dbQuote(args[a]))
      inc(a)
    else:
      add(result, c)

proc query*(conn: Connection, query: SqlQuery, args: varargs[string, `$`], onlyFirst = false): Future[ResultSet[string]] {.
            async, #[tags: [ReadDbEffect]]#.} =
  var q = dbFormat(query, args)
  result = await conn.rawQuery(q, onlyFirst)

proc tryQuery*(conn: Connection, query: SqlQuery, args: varargs[string, `$`]): Future[bool] {.
               async, #[tags: [ReadDbEffect]]#.} =
  ## tries to execute the query and returns true if successful, false otherwise.
  result = true
  try:
    discard await conn.query(query, args)
  except:
    result = false
  return result

proc getRow*(conn: Connection, query: SqlQuery,
             args: varargs[string, `$`]): Future[Row] {.async, #[tags: [ReadDbEffect]]#.} =
  ## Retrieves a single row. If the query doesn't return any rows, this proc
  ## will return a Row with empty strings for each column.
  let resultSet = await conn.query(query, args, onlyFirst = true)
  if resultSet.rows.len == 0:
    let cols = resultSet.columns.len
    result = newSeq[string](cols)
  else:
    result = resultSet.rows[0]

proc getAllRows*(conn: Connection, query: SqlQuery,
                 args: varargs[string, `$`]): Future[seq[Row]] {.async, #[tags: [ReadDbEffect]]#.} =
  ## executes the query and returns the whole result dataset.
  let resultSet = await conn.query(query, args)
  result = resultSet.rows

proc getValue*(conn: Connection, query: SqlQuery,
               args: varargs[string, `$`]): Future[string] {.async, #[tags: [ReadDbEffect]]#.} =
  ## executes the query and returns the first column of the first row of the
  ## result dataset. Returns "" if the dataset contains no rows or the database
  ## value is NULL.
  let row = await getRow(conn, query, args)
  result = row[0]

proc tryInsertId*(conn: Connection, query: SqlQuery,
                  args: varargs[string, `$`]): Future[int64] {.async, #[raises: [], tags: [WriteDbEffect]]#.} =
  ## executes the query (typically "INSERT") and returns the
  ## generated ID for the row or -1 in case of an error.
  var resultSet:ResultSet[string]
  try:
    resultSet = await conn.query(query, args)
  except:
    result = -1'i64
    return result
  result = resultSet.status.last_insert_id.int64

proc insertId*(conn: Connection, query: SqlQuery,
               args: varargs[string, `$`]): Future[int64] {.async, #[tags: [WriteDbEffect]]#.} =
  ## executes the query (typically "INSERT") and returns the
  ## generated ID for the row.
  let resultSet = await conn.query(query, args)
  result = resultSet.status.last_insert_id.int64

proc tryInsert*(conn: Connection, query: SqlQuery, pkName: string,
                args: varargs[string, `$`]): Future[int64] {.async,#[raises: [], tags: [WriteDbEffect]]#.} =
  ## same as tryInsertID
  result = await tryInsertID(conn, query, args)

proc insert*(conn: Connection, query: SqlQuery, pkName: string,
             args: varargs[string, `$`]): Future[int64]
            {.async, #[tags: [WriteDbEffect]]#.} =
  ## same as insertId
  let resultSet = await conn.query(query, args)
  result = resultSet.status.last_insert_id.int64

proc setEncoding*(conn: Connection, encoding: string): Future[bool] {.async, #[raises: [], tags: [DbEffect]]#.} =
  ## sets the encoding of a database connection, returns true for
  ## success, false for failure.
  result = await conn.tryQuery(sql"SET NAMES ?",encoding)

proc handleParams(conn: Connection, q: string) {.async.} =
  ## SHOW VARIABLES;
  ## https://dev.mysql.com/doc/refman/8.0/en/using-system-variables.html
  var key,val:string
  var cmd = "SET "
  var pos = 0
  for item in split(q,"&"):
    (key,val) = item.split("=")
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
      cmd.add key
      cmd.add '='
      cmd.add val
      inc pos
  discard await conn.rawQuery cmd

proc open*(uriStr: string | Uri): Future[Connection] {.async.} =
  ## https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html
  let uri = when uriStr is string: parseUri(uriStr) else: uriStr
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
  await conn.sendPacket(buf, reset_seq_no=true)
  discard await conn.receivePacket(drop_ok=true)
  conn.socket.close()