
import strutils
import endians
import ./errors

const
  LenEnc_16        = 0xFC
  LenEnc_24        = 0xFD
  LenEnc_64        = 0xFE

# These are protocol constants; see
#  https://dev.mysql.com/doc/internals/en/overview.html

const
  ResponseCode_OK*  : uint8 = 0
  ResponseCode_EOF* : uint8 = 254   # Deprecated in mysql 5.7.5
  ResponseCode_ERR* : uint8 = 255
  ResponseCode_LOCAL_INFILE* : uint8 = 251 # 0xfb
  ResponseCode_AuthSwitchRequest*: uint8 = 254 # 0xFE
  ResponseCode_ExtraAuthData*: uint8 = 1 # 0x01
  NullColumn*       = char(0xFB)

  HandshakeV10 : uint8 = 0x0A  # 10 Initial handshake packet since MySQL 3.21

  Charset_swedish_ci : uint8 = 0x08
  Charset_utf8_ci*    : uint8 = 0x21
  Charset_binary     : uint8 = 0x3f

type
  nat24 = range[0 .. 16777215]
  SessionStateType* {.pure.} = enum
    systemVariables = 0.uint8
    schema = 1.uint8
    stateChange = 2.uint8
    gtids = 3.uint8
    transactionCharacteristics = 4.uint8
    transactionState = 5.uint8
  SessionState* = object
    name*: string
    typ*: SessionStateType
    value*: string
  Status {.pure.} = enum
    inTransaction = 0  # a transaction is active
    autoCommit = 1 # auto-commit is enabled
    moreResultsExist = 3
    noGoodIndexUsed = 4
    noIndexUsed = 5
    cursorExists = 6 # Used by Binary Protocol Resultset
    lastRowSent = 7
    dbDropped = 8
    noBackslashEscapes = 9
    metadataChanged = 10
    queryWasSlow = 11
    psOutParams = 12
    inTransactionReadOnly = 13 # in a read-only transaction
    sessionStateChanged = 14 # connection state information has changed

  # These correspond to the CMD_FOO definitions in mysql.
  # Commands marked "internal to the server", and commands
  # only used by the replication protocol, are commented out
  # https://dev.mysql.com/worklog/task/?id=8754
  # Following COM_XXX commands can be deprecated as there are alternative sql 
  # statements associated with them.
  # COM_FIELD_LIST (show columns sql statement)
  # COM_REFRESH (flush sql statement)
  # COM_PROCESS_INFO(show processlist sql statement)
  # COM_PROCESS_KILL (kill connection/query sql statement)
  Command* {.pure.} = enum
    # sleep = 0
    quit = 1
    initDb = 2
    query = 3
    fieldList = 4     # Deprecated, show fields sql statement
    createDb = 5      # Deprecated, create table sql statement
    dropDb = 6        # Deprecated, drop table sql statement
    refresh = 7       # Deprecated, flush sql statement
    shutdown = 8
    statistics = 9
    processInfo = 10  # Deprecated, show processlist sql statement
    # connect = 11
    processKill = 12  # Deprecated, kill connection/query sql statement
    debug = 13
    ping = 14
    # time = 15
    # delayedInsert = 16
    changeUser = 17

    # Replication commands
    # binlogDump = 18
    # tableDump = 19
    # connectOut = 20
    # registerSlave = 21
    # binlogDumpGtid = 30

    # Prepared statements
    statementPrepare = 22
    statementExecute = 23
    statementSendLongData = 24
    statementClose = 25
    statementReset = 26

    # Stored procedures
    setOption = 27
    statementFetch = 28

    # daemon = 29
    resetConnection = 31
    
  ## https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html
  FieldFlag* {.pure.} = enum
    notNull = 0 # Field can't be NULL
    primaryKey = 1 # Field is part of a primary key
    uniqueKey = 2 # Field is part of a unique key
    multipleKey = 3 # Field is part of a key
    blob = 4 # Field is a blob
    unsigned = 5 # Field is unsigned
    zeroFill = 6 # Field is zerofill
    binary = 7 # Field is binary

    # The following are only sent to new clients (what is "new"? 4.1+?)
    enumeration = 8 # field is an enum
    autoIncrement = 9 # field is a autoincrement field
    timeStamp = 10 # Field is a timestamp
    isSet = 11 # Field is a set
    noDefaultValue = 12 # Field doesn't have default value
    onUpdateNow = 13 # Field is set to NOW on UPDATE
    isNum = 15 # Field is num (for clients)

  FieldType* = enum
    fieldTypeDecimal     = uint8(0)
    fieldTypeTiny        = uint8(1)
    fieldTypeShort       = uint8(2)
    fieldTypeLong        = uint8(3)
    fieldTypeFloat       = uint8(4)
    fieldTypeDouble      = uint8(5)
    fieldTypeNull        = uint8(6)
    fieldTypeTimestamp   = uint8(7)
    fieldTypeLongLong    = uint8(8)
    fieldTypeInt24       = uint8(9)
    fieldTypeDate        = uint8(10)
    fieldTypeTime        = uint8(11)
    fieldTypeDateTime    = uint8(12)
    fieldTypeYear        = uint8(13)
    fieldTypeVarchar     = uint8(15)
    fieldTypeBit         = uint8(16)
    fieldTypeJson        = uint8(245)
    fieldTypeNewDecimal  = uint8(246)
    fieldTypeEnum        = uint8(247)
    fieldTypeSet         = uint8(248)
    fieldTypeTinyBlob    = uint8(249)
    fieldTypeMediumBlob  = uint8(250)
    fieldTypeLongBlob    = uint8(251)
    fieldTypeBlob        = uint8(252)
    fieldTypeVarString   = uint8(253)
    fieldTypeString      = uint8(254)
    fieldTypeGeometry    = uint8(255)

  CursorType* {.pure.} = enum
    noCursor             = 0
    readOnly             = 1
    forUpdate            = 2
    scrollable           = 3

  # Server response packets: OK and EOF
  ResponseOK* {.final.} = object 
    eof               : bool  # True if EOF packet, false if OK packet
    affectedRows*    : Natural
    lastInsertId*   : Natural
    statusFlags*     : set[Status]
    warningCount*    : Natural
    info*             : string
    sessionStateChanges*: seq[SessionState]
  ResponseAuthSwitch* {.final.} = object 
    status: uint8 # const ResponseCode_AuthSwitchRequest
    pluginName*: string
    pluginData*: string
  ResponseAuthMore* {.final.} = object
    status: uint8 # const 0x01
    pluginData*: string

  # Server response packet: ERR (which can be thrown as an exception)
  ResponseERR* = object of CatchableError
    error_code: uint16
    sqlstate: string
 
  HandshakePacket* = ref HandshakePacketObj
  HandshakePacketObj = object       
    ## Packet from mysql server when connecting to the server that requires authentication.
    ## see https://dev.mysql.com/doc/internals/en/connection-phase-packets.html
    protocolVersion*: int      # 1
    serverVersion*: string     # NullTerminatedString
    threadId*: int             # 4 connection id
    scrambleBuff1*: string      # 8 # auth_plugin_data_part_1
    capabilities*: int         # (4)
    capabilities1*: int         # 2
    charset*: int              # 1
    serverStatus*: int         # 2
    capabilities2*: int         # [2]
    scrambleLen*: int          # [1]
    scrambleBuff2*: string      # [12]
    scrambleBuff*: string      # 8 + [12]
    plugin*: string            # NullTerminatedString 
    protocol41*: bool
type 
  ColumnDefinition* {.final.} = object 
    catalog*     : string
    schema*      : string
    table*       : string
    origTable*  : string
    name*        : string
    origName*   : string
    charset*      : int16
    length*      : uint32
    columnType* : FieldType
    flags*       : set[FieldFlag]
    decimals*    : int
  
  ResultSet*[T] {.final.} = object 
    status*     : ResponseOK
    columns*    : seq[ColumnDefinition]
    rows*       : seq[seq[T]]
## Basic datatype packers/unpackers
## little endian
# Integers

proc setInt32*(buf: var openarray[char], pos:int, value:int) {.inline.} =
  buf[pos] = char( (value and 0xFF) )
  buf[pos + 1] = char( ((value shr 8) and 0xFF) )
  buf[pos + 2] = char( ((value shr 16) and 0xFF) )

proc scan16(buf: openarray[char], pos: int , p: pointer) {.inline.} =
  when system.cpuEndian == bigEndian:
    swapEndian16(p, buf[pos].addr)
  else:
    copyMem(p, buf[pos].unSafeAddr, 2)

proc put16(buf: var string, p: pointer) {.inline.} =
  var arr:array[0..1, char]
  littleEndian16(addr arr, p)
  let bufLen = buf.len
  buf.setLen(bufLen + 2)
  copyMem(buf[bufLen].addr, arr[0].addr, 2)

proc scan32(buf: openarray[char], pos: int , p: pointer) {.inline.} =
  when system.cpuEndian == bigEndian:
    swapEndian32(p, buf[pos].addr)
  else:
    copyMem(p, buf[pos].unSafeAddr, 4)

proc put32(buf: var string, p: pointer) {.inline.} =
  var arr:array[0..3, char]
  littleEndian32(addr arr, p)
  let bufLen = buf.len
  buf.setLen(bufLen + 4)
  copyMem(buf[bufLen].addr, arr[0].addr, 4)

proc scan64(buf: openarray[char], pos: int , p: pointer) {.inline.} =
  when system.cpuEndian == bigEndian:
    swapEndian64(p, buf[pos].addr)
  else:
    copyMem(p, buf[pos].unSafeAddr, 8)

proc put64(buf: var string, p: pointer) {.inline.} =
  var arr:array[0..7, char]
  littleEndian64(addr arr, p)
  let bufLen = buf.len
  buf.setLen(bufLen + 8)
  copyMem(buf[bufLen].addr, arr[0].addr, 8)

proc putU8(buf: var string, val: uint8) {.inline.} =
  buf.add( char(val) )

proc putU8*(buf: var string, val: range[0..255]) {.inline.} =
  buf.add( char(val) )
  
proc scanU16*(buf: openarray[char], pos: int): uint16 =
  scan16(buf, pos, result.addr)

proc putU16*(buf: var string, val: uint16) =
  put16(buf, val.unSafeAddr)

proc scanU32*(buf: openarray[char], pos: int): uint32 =
  scan32(buf, pos, addr result)

proc putU32*(buf: var string, val: uint32) =
  put32(buf, val.unSafeAddr)

proc putFloat*(buf: var string, val:float32) =
  var str = newString(4)
  copyMem(str[0].addr, val.unSafeAddr, 4)
  buf.add str

proc putDouble*(buf: var string, val: float64) =
  var uval = cast[ptr uint64](val.unSafeAddr)
  put64(buf, uval)

proc scanFloat*(buf: openarray[char], pos: int): float32 =
  scan32(buf, pos, addr result)

proc scanDouble*(buf: openarray[char], pos: int): float64 =
  scan64(buf, pos, addr result)

proc scanU64*(buf: openarray[char], pos: int): uint64 =
  scan64(buf, pos, addr result)

proc putS64*(buf: var string, val: int64) =
  put64(buf, val.unSafeAddr)

proc putU64*(buf: var string, val: uint64) =
  put64(buf, val.unSafeAddr)

proc readLenInt*(buf: openarray[char], pos: var int): int =
  let b1 = uint8(buf[pos])
  if b1 < 251:
    inc(pos)
    return int(b1)
  if b1 == LenEnc_16:
    result = int(uint16(buf[pos+1]) + ( uint16(buf[pos+2]) shl 8 ))
    pos = pos + 3
    return
  if b1 == LenEnc_24:
    result = int(uint32(buf[pos+1]) + ( uint32(buf[pos+2]) shl 8 ) + ( uint32(buf[pos+3]) shl 16 ))
    pos = pos + 4
    return
  return -1


proc putLenInt*(buf: var string, val: int|uint|int32|uint32):int {.discardable.} =
  # https://dev.mysql.com/doc/dev/mysql-server/8.0.19/page_protocol_basic_dt_integers.html
  # for string and raw data
  if val < 0:
    raise newException(ProtocolError, "trying to send a negative lenenc-int")
  elif val < 251:
    buf.add( char(val) )
    return 1
  elif val < 65536:
    buf.add( char(LenEnc_16) )
    buf.add( char( val and 0xFF ) )
    buf.add( char( (val shr 8) and 0xFF ) )
    return 3
  elif val <= 0xFFFFFF: # 16777215
    buf.add( char(LenEnc_24) )
    buf.add( char( val and 0xFF ) )
    buf.add( char( (val shr 8) and 0xFF ) )
    buf.add( char( (val shr 16) and 0xFF ) )
    return 4
  else:
    raise newException(ProtocolError, "lenenc-int too long for me!")

proc countLenInt*( val: int|uint|int32|uint32):int =
  # https://dev.mysql.com/doc/dev/mysql-server/8.0.19/page_protocol_basic_dt_integers.html
  # for string and raw data
  if val < 0:
    raise newException(ProtocolError, "trying to send a negative lenenc-int")
  elif val < 251:
    return 1
  elif val < 65536:
    return 3
  elif val <= 0xFFFFFF: # 16777215
    return 4
  else:
    raise newException(ProtocolError, "lenenc-int too long for me!")


# Strings
proc readNulString*(buf: openarray[char], pos: var int): string =
  result = ""
  while buf[pos] != char(0):
    result.add(buf[pos])
    inc(pos)
  inc(pos)

proc readNulStringX*(buf: openarray[char], pos: var int): string =
  # scan null string limited to buf high
  result = ""
  while pos <= high(buf) and buf[pos] != char(0):
    result.add(buf[pos])
    inc(pos)
  inc(pos)

proc putNulString*(buf: var string, val: string) =
  buf.add(val)
  buf.add( char(0) )

proc readLenStr*(buf: openarray[char], pos: var int): string =
  let slen = readLenInt(buf, pos)
  if slen < 0:
    raise newException(ProtocolError, "lenenc-int: is 0x" & toHex(int(buf[pos]), 2))
  result = cast[string](buf[pos .. pos+slen-1])
  pos = pos + slen

proc putLenStr*(buf: var string, val: string) =
  putLenInt(buf, val.len)
  buf.add(val)

proc writeTypeAndFlag*(buf :var string, intVal: int64) {.inline.} = 
  if intVal >= 0:
    if intVal < 256'i64:
      buf.add(char(fieldTypeTiny))
    elif intVal < 65536'i64:
      buf.add(char(fieldTypeShort))
    elif intVal < (65536'i64 * 65536'i64):
      buf.add(char(fieldTypeLong))
    else:
      buf.add(char(fieldTypeLongLong))
    buf.add(char(0x80))
  else:
    if intVal >= -128:
      buf.add(char(fieldTypeTiny))
    elif intVal >= -32768:
      buf.add(char(fieldTypeShort))
    else:
      buf.add(char(fieldTypeLongLong))
    buf.add(char(0))

proc writeTypeAndFlag*(buf: var string, intVal: uint64) {.inline.} = 
  if intVal < (65536'u64 * 65536'u64):
    buf.add(char(fieldTypeLong))
  else:
    buf.add(char(fieldTypeLongLong))
  buf.add(char(0x80))

proc putValue*(buf: var string, intVal: int64) = 
  if intVal >= 0:
    buf.putU8(intVal and 0xFF)
    if intVal >= 256:
      buf.putU8((intVal shr 8) and 0xFF)
      if intVal >= 65536:
        buf.putU16( (intVal shr 16).uint16 and 0xFFFF'u16)
        if intVal >= (65536'i64 * 65536'i64):
          buf.putU32(uint32(intVal shr 32))
  else:
    if intVal >= -128:
      buf.putU8(uint8(intVal + 256))
    elif intVal >= -32768:
      buf.putU16(uint16(intVal + 65536))
    else:
      buf.putS64(intVal)

proc putValue*(buf: var string, intVal: uint64) = 
  putU32(buf, uint32(intVal and 0xFFFFFFFF'u64))
  if intVal >= 0xFFFFFFFF'u64:
    putU32(buf, uint32(intVal shr 32))

proc writeTypeAndFlag*(buf: var string, fieldType: FieldType) {.inline.} = 
  const isUnsigned = char(0)
  buf.add fieldType.char
  buf.add isUnsigned

when isMainModule or defined(test):
  proc hexstr(s: string): string =
    const HexChars = "0123456789abcdef"
    result = newString(s.len * 2)
    for pos, c in s:
      var n = ord(c)
      result[pos * 2 + 1] = HexChars[n and 0xF]
      n = n shr 4
      result[pos * 2] = HexChars[n]
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
  assert "0001fafcfb00fcfc00fc0002fc8002fcfffffd000001fdabcdefffff000000000100aa0ac080" == hexstr(buf)
  var pos: int = 0

  assert 0 == readLenInt(buf, pos)
  assert 1    == readLenInt(buf, pos)
  assert 250  == readLenInt(buf, pos)
  assert 251  == readLenInt(buf, pos)
  assert 252  == readLenInt(buf, pos)
  assert 512  == readLenInt(buf, pos)
  assert 640  == readLenInt(buf, pos)
  assert 0x0FFFF == readLenInt(buf, pos)
  assert 0x10000 ==  readLenInt(buf, pos)
  assert 15715755 ==  readLenInt(buf, pos)
  assert 65535 ==  int(scanU32(buf, pos))
  assert 65535'u16 ==  scanU16(buf, pos)
  assert 255'u16 ==  scanU16(buf, pos+1)
  assert 0'u16 ==  scanU16(buf, pos+2)
  pos += 4
  assert 65536 == int(scanU32(buf, pos))
  pos += 4
  assert 0x80C00AAA ==  int(scanU32(buf, pos))
  pos += 4
  assert 0x80C00AAA00010000'u64 ==  scanU64(buf, pos-8)
  assert len(buf) ==  pos
