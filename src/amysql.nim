##
## This module implements (a subset of) the MySQL/MariaDB client
## protocol based on asyncnet and asyncdispatch.
##
## No attempt is made to make this look like the C-language
## libmysql API.
##
## Copyright (c) 2015 William Lewis
## Copyright (c) 2020 Bung
{.experimental: "views".}
import amysql/private/protocol
export protocol
import amysql/private/format
import amysql/conn
export conn
import amysql/conn_connection
export conn_connection
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
export db_common
import strutils
import amysql/async_varargs
import uri
import times
import json

import logging


var consoleLog = newConsoleLogger()
addHandler(consoleLog)
when defined(release):  setLogFilter(lvlInfo)

type
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
 
  SqlPrepared* = ref SqlPreparedObj
  SqlPreparedObj = object
    statement_id: array[4, char]
    parameters: seq[ColumnDefinition]
    columns: seq[ColumnDefinition]
    warnings: Natural
    conn: Connection
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

# Parameter and result packers/unpackers

proc approximatePackedSize(p: SqlParam): int {.inline.} =
  ## approximate packed size for reducing reallocations
  case p.typ
  of paramNull: result = 0
  of paramString, paramBlob, paramJson, paramGeometry:
    result = 5 + len(p.strVal)
  of paramInt, paramUInt, paramFloat, paramDate, paramTimestamp: 
    result = 4 + 1
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
    return $v.datetimeVal.toTime.toUnix
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
  of rvtDate:
    return v.datetimeVal.format("yyyy-MM-dd")
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

proc parseTextRow(pkt: openarray[char]): seq[string] =
  var pos = 0
  result = newSeq[string]()
  while pos < len(pkt):
    if pkt[pos] == NullColumn:
      result.add("")
      inc(pos)
    else:
      result.add(pkt.readLenStr(pos))

proc prepare*(conn: Connection, qs: string): Future[SqlPrepared] {.async.} =
  var buf: string = newStringOfCap(4 + 1 + len(qs))
  buf.setLen(4)
  buf.add( char(Command.statementPrepare) )
  buf.add(qs)
  await conn.sendPacket(buf, resetSeqId=true)
  let pkt = await conn.receivePacket()
  if isERRPacket(pkt):
    raise parseErrorPacket(pkt)
  if pkt[0] != char(ResponseCode_OK) or len(pkt) < 12:
    raise newException(ProtocolError, "Unexpected response to STMT_PREPARE (len=" & $(pkt.len) & ", first byte=0x" & toHex(int(pkt[0]), 2) & ")")
  let numColumns = scanU16(pkt, 5)
  let numParams = scanU16(pkt, 7)
  let numWarnings = scanU16(pkt, 10)

  new(result)
  result.warnings = numWarnings
  for b in 0 .. 3: result.statement_id[b] = pkt[1+b]
  var pos = 12
  let pktLen = pkt.len()
  if numParams > 0'u16:
    debug "prepare receiveMetadata numParams:" & $numParams
    if pktLen > pos:
      var index = 0
      result.parameters = newSeq[ColumnDefinition](numParams)
      while index < numParams.int:
        inc(pos,4)
        processMetadata(result.parameters, index, pkt, pos)
        inc index
    else:
      result.parameters = await conn.receiveMetadata(int(numParams))
  else:
    result.parameters = newSeq[ColumnDefinition](0)
  if numColumns > 0'u16:
    debug "prepare receiveMetadata numColumns:" & $numColumns
    if pktLen > pos:
      var index = 0
      result.columns = newSeq[ColumnDefinition](numColumns)
      while index < numColumns.int:
        inc(pos,4)
        processMetadata(result.columns, index, pkt, pos)
        inc index
    else:
      result.columns = await conn.receiveMetadata(int(numColumns))

proc prepare(pstmt: SqlPrepared, buf: var string, cmd: Command, cap: int = 9) =
  buf = newStringOfCap(cap)
  buf.setLen(9)
  buf[4] = char(cmd)
  for b in 0..3: buf[b+5] = pstmt.statement_id[b]

proc finalize*(conn: Connection, pstmt: SqlPrepared): Future[void] =
  var buf: string
  pstmt.prepare(buf, Command.statementClose)
  return conn.sendPacket(buf, resetSeqId=true)

proc reset*(conn: Connection, pstmt: SqlPrepared): Future[void] =
  var buf: string
  pstmt.prepare(buf, Command.statementReset)
  return conn.sendPacket(buf, resetSeqId=true)

proc formatBoundParams*(conn: Connection, pstmt: SqlPrepared, params: openarray[SqlParam]): string =
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

proc parseBinaryRow( pkt: openarray[char] ,columns: seq[ColumnDefinition]): seq[ResultValue] =
  ## see https://mariadb.com/kb/en/resultset-row/
  ## https://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html
  
  # For the Binary Protocol Resultset Row the num-fields and the field-pos need to add a offset of 2.
  # For COM_STMT_EXECUTE this offset is 0.
  const offset = 2 
  let columnCount = columns.len
  let bitmapBytes = (columnCount + offset + 7) div 8
  if len(pkt) < (1 + bitmapBytes) or pkt[0] != char(0):
    raise newException(ProtocolError, "Truncated or incorrect binary result row")
  newSeq(result, columnCount)
  var pos = 1 + bitmapBytes
  for ix in 0 .. columnCount-1:
    # First, check whether this column's bit is set in the null
    # bitmap.
    # https://dev.mysql.com/doc/internals/en/null-bitmap.html
    let fieldIndex = ix + offset
    let bytePos = fieldIndex div 8
    let bitPos = fieldIndex mod 8
    let bitmap = uint8(pkt[ 1 + bytePos ])
    if (bitmap and uint8(1 shl bitPos)) != 0'u8:
      result[ix] = ResultValue(typ: rvtNull)
    else:
      let typ = columns[ix].columnType
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
  return conn.sendPacket(pkt, resetSeqId=true)

template processResultset(conn: Connection, pkt:openarray[char], pos:var int, result: typed,isFirst:static[bool],onlyFirst:typed, isTextMode:static[bool], process:untyped): untyped {.dirty.} =
  when not isFirst:
    inc(pos,4)
  let columnCount = readLenInt(pkt, pos)
  debug "result.columns len" &  $result.columns.len
  if conn.use_zstd():
    var index = 0
    result.columns = newSeq[ColumnDefinition](columnCount)
    while index < columnCount.int:
      inc(pos,4)
      processMetadata(result.columns, index, pkt, pos)
      inc index
  else:
    result.columns = await conn.receiveMetadata(columnCount)
  debug $result
  # var pkt1: openarray[char]
  var pkt1: seq[char]
  var pkt1Len:int
  let fullLen = pkt.len
  while true:
    if conn.use_zstd():
      if pos >= fullLen:
        break
      pkt1Len = int32( uint32(pkt[pos]) or (uint32(pkt[pos + 1]) shl 8) or (uint32(pkt[pos + 2]) shl 16) )
      inc(pos,4)
      pkt1 = pkt[pos ..< pos + pkt1Len] # pkt[0].unsafeAddr.toOpenArray(pos, pos + pkt1Len - 1)
      inc(pos,pkt1Len)
    else:
      pkt1 = await conn.receivePacket()
      # pkt1 = pkt[0].unsafeAddr.toOpenArray(0, pkt.len - 1)
    if isEOFPacket(pkt1):
        result.status = parseEOFPacket(pkt1)
        debug result.status.statusFlags
        if conn.use_zstd():
          discard
        else:
          break
    elif isTextMode and isOKPacket(pkt1):
      result.status = parseOKPacket(conn, pkt1)
      break
    elif isERRPacket(pkt1):
      raise parseErrorPacket(pkt1)
    else:
      process
      if onlyFirst:
        continue

template fetchResultset(conn:Connection, pkt:typed, result:typed, onlyFirst:typed, isTextMode:static[bool], process:untyped): untyped {.dirty.} =
  var pos = 0
  let pktLenA = pkt.len
  processResultset(conn,pkt,pos,result,true,onlyFirst,isTextMode,process)
  while pos < pktLenA - 1:
    processResultset(conn,pkt,pos,result,false,onlyFirst,isTextMode,process)

{.push warning[ObservableStores]: off.}
proc rawExec*(conn: Connection, qs: string): Future[ResultSet[string]] {.
               async,#[ tags: [ReadDbEffect, WriteDbEffect,RootEffect]]#.} =
  await conn.sendQuery(qs)
  let pkt = await conn.receivePacket()
  if isOKPacket(pkt):
    # Success, but no rows returned.
    result.status = parseOKPacket(conn, pkt)
  elif isERRPacket(pkt):
    raise parseErrorPacket(pkt)
  else: 
    conn.fetchResultset(pkt, result, onlyFirst = false, isTextMode = true): discard

proc rawQuery*(conn: Connection, qs: string, onlyFirst:bool = false): Future[ResultSet[string]] {.
               async, #[ tags: [ReadDbEffect, WriteDbEffect,RootEffect]]#.} =
  await conn.sendQuery(qs)
  let pkt = await conn.receivePacket()
  debug repr pkt
  if isOKPacket(pkt):
    # Success, but no rows returned.
    result.status = parseOKPacket(conn, pkt)
  elif isERRPacket(pkt):
    raise parseErrorPacket(pkt)
  elif isEOFPacket(pkt):
    result.status = parseEOFPacket(pkt)
  else:
    conn.fetchResultset(pkt, result, onlyFirst, true, result.rows.add(parseTextRow(pkt1)))

proc performPreparedQuery*(conn: Connection, pstmt: SqlPrepared, st: Future[void], onlyFirst:static[bool] = false): Future[ResultSet[ResultValue]] {.
                          async#[, tags:[RootEffect]]#.} =
  await st
  let pkt = await conn.receivePacket()
  if isOKPacket(pkt):
    # Success, but no rows returned.
    result.status = parseOKPacket(conn, pkt)
  elif isERRPacket(pkt):
    raise parseErrorPacket(pkt)
  else:
    conn.fetchResultset(pkt, result, onlyFirst,false, result.rows.add(parseBinaryRow( pkt1 ,result.columns)))
{.pop.}

proc query*(conn: Connection, pstmt: SqlPrepared, params: varargs[SqlParam, asParam]): Future[ResultSet[ResultValue]] {.
            #[tags: [ReadDbEffect, WriteDbEffect]]#.} =
  var pkt = conn.formatBoundParams(pstmt, @params)
  var sent = conn.sendPacket(pkt, resetSeqId=true)
  return performPreparedQuery(conn, pstmt, sent)

proc selectDatabase*(conn: Connection, database: string): Future[ResponseOK] {.async.} =
  var buf: string = newStringOfCap(4 + 1 + len(database))
  buf.setLen(4)
  buf.add( Command.initDb.char )
  buf.add(database)
  await conn.sendPacket(buf, resetSeqId=true)
  let pkt = await conn.receivePacket()
  if isERRPacket(pkt):
    raise parseErrorPacket(pkt)
  elif isOKPacket(pkt):
    return parseOKPacket(conn, pkt)
  elif isEOFPacket(pkt):
    return parseEOFPacket(pkt)
  else:
    raise newException(ProtocolError, "unexpected response to COM_INIT_DB:" & cast[string](pkt))

proc exec*(conn: Connection, qs: SqlQuery, args: varargs[string, `$`]): Future[ResultSet[string]] {.
            asyncVarargs.} =
  var q = dbFormat(qs, args)
  result = await conn.rawExec(q)

proc query*(conn: Connection, qs: SqlQuery, args: varargs[string, `$`], onlyFirst: static[bool] = false): Future[ResultSet[string]] {.asyncVarargs.} =
  var q = dbFormat(qs, args)
  result = await conn.rawQuery(q, onlyFirst)

proc tryQuery*(conn: Connection, qs: SqlQuery, args: varargs[string, `$`]): Future[bool] {.
               asyncVarargs, #[tags: [ReadDbEffect]]#.} =
  ## tries to execute the query and returns true if successful, false otherwise.
  result = true
  try:
    discard await conn.exec(qs, args)
  except:
    result = false
  return result

proc getRow*(conn: Connection, qs: SqlQuery,
             args: varargs[string, `$`]): Future[Row] {.asyncVarargs,  #[tags: [ReadDbEffect]]#.} =
  ## Retrieves a single row. If the query doesn't return any rows, this proc
  ## will return a Row with empty strings for each column.
  let resultSet = await conn.query(qs, args, onlyFirst = true)
  if resultSet.rows.len == 0:
    let cols = resultSet.columns.len
    result = newSeq[string](cols)
  else:
    result = resultSet.rows[0]

proc getAllRows*(conn: Connection, qs: SqlQuery,
                 args: varargs[string, `$`]): Future[seq[Row]] {.asyncVarargs,  #[tags: [ReadDbEffect]]#.} =
  ## executes the query and returns the whole result dataset.
  let resultSet = await conn.query(qs, args)
  result = resultSet.rows

proc getValue*(conn: Connection, qs: SqlQuery,
               args: varargs[string, `$`]): Future[string] {.asyncVarargs,  #[tags: [ReadDbEffect]]#.} =
  ## executes the query and returns the first column of the first row of the
  ## result dataset. Returns "" if the dataset contains no rows or the database
  ## value is NULL.
  let row = await getRow(conn, qs, args)
  result = row[0]

proc tryInsertId*(conn: Connection, qs: SqlQuery,
                  args: varargs[string, `$`]): Future[int64] {.asyncVarargs, #[raises: [], tags: [WriteDbEffect]]#.} =
  ## executes the query (typically "INSERT") and returns the
  ## generated ID for the row or -1 in case of an error.
  var resultSet:ResultSet[string]
  try:
    resultSet = await conn.exec(qs, args)
  except:
    result = -1'i64
    return result
  result = resultSet.status.lastInsertId.int64

proc insertId*(conn: Connection, qs: SqlQuery,
               args: varargs[string, `$`]): Future[int64] {.asyncVarargs,  #[tags: [WriteDbEffect]]#.} =
  ## executes the query (typically "INSERT") and returns the
  ## generated ID for the row.
  let resultSet = await conn.exec(qs, args)
  result = resultSet.status.lastInsertId.int64

proc tryInsert*(conn: Connection, qs: SqlQuery, pkName: string,
                args: varargs[string, `$`]): Future[int64] {.asyncVarargs, #[raises: [], tags: [WriteDbEffect]]#.} =
  ## same as tryInsertID
  result = await tryInsertID(conn, qs, args)

proc insert*(conn: Connection, qs: SqlQuery, pkName: string,
             args: varargs[string, `$`]): Future[int64]
            {.asyncVarargs,  #[tags: [WriteDbEffect]]#.} =
  ## same as insertId
  let resultSet = await conn.exec(qs, args)
  result = resultSet.status.lastInsertId.int64

proc setEncoding*(conn: Connection, encoding: string): Future[bool] {.async,  #[raises: [], tags: [DbEffect]]#.} =
  ## sets the encoding of a database connection, returns true for
  ## success, false for failure.
  result = await conn.tryQuery(sql"SET NAMES ?",encoding)

proc startTransaction*(conn: Connection) {.async, inline.} =
  discard await conn.rawExec("START TRANSACTION")

proc commit*(conn: Connection) {.async, inline.} =
  discard await conn.rawExec("COMMIT")

proc rollback*(conn: Connection) {.async, inline.} =
  discard await conn.rawExec("ROLLBACK")

template transaction*(conn: typed, process: untyped) =
  ## experimental
  discard await conn.rawExec("START TRANSACTION")
  process
  discard await conn.rawExec("COMMIT")
