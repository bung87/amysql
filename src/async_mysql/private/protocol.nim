
import ./protocol_basic
export protocol_basic
import ./cap
import asyncnet,asyncdispatch
import ../conn

# These are protocol constants; see
#  https://dev.mysql.com/doc/internals/en/overview.html

const
  ResponseCode_OK*  : uint8 = 0
  ResponseCode_EOF* : uint8 = 254   # Deprecated in mysql 5.7.5
  ResponseCode_ERR* : uint8 = 255
  ResponseCode_AuthSwitchRequest*: uint8 = 254 # 0xFE
  ResponseCode_ExtraAuthData*: uint8 = 1 # 0x01
  NullColumn*       = char(0xFB)

  HandshakeV10 : uint8 = 0x0A  # Initial handshake packet since MySQL 3.21

  Charset_swedish_ci : uint8 = 0x08
  Charset_utf8_ci*    : uint8 = 0x21
  Charset_binary     : uint8 = 0x3f


type
  nat24 = range[0 .. 16777215]

  Status* {.pure.} = enum
    inTransaction = 1  # a transaction is active
    autoCommit = 2 # auto-commit is enabled
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
    affected_rows*    : Natural
    last_insert_id*   : Natural
    status_flags*     : set[Status]
    warning_count*    : Natural
    info*             : string
    # session_state_changes: seq[ ... ]
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
  HandshakeState* = enum # Parse state for handshaking.
    hssProtocolVersion, 
    hssServerVersion, 
    hssThreadId, 
    hssScrambleBuff1,   
    hssFiller0,       
    hssCapabilities1, 
    hssCharSet,         
    hssStatus,        
    hssCapabilities2, 
    hssFiller1,         
    hssFiller2,       
    hssScrambleBuff2, 
    hssFiller3,         
    hssPlugin
  HandshakePacket* = ref HandshakePacketObj
  HandshakePacketObj = object       
    ## Packet from mysql server when connecting to the server that requires authentication.
    ## see https://dev.mysql.com/doc/internals/en/connection-phase-packets.html
    sequenceId*: int           # 1
    protocolVersion*: int      # 1
    serverVersion*: string     # NullTerminatedString
    threadId*: int             # 4
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
    state*: HandshakeState
# EOF is signaled by a packet that starts with 0xFE, which is
# also a valid length-encoded-integer. In order to distinguish
# between the two cases, we check the length of the packet: EOFs
# are always short, and an 0xFE in a result row would be followed
# by at least 65538 bytes of data.
proc isEOFPacket*(pkt: string): bool =
  result = (len(pkt) >= 1) and (pkt[0] == char(ResponseCode_EOF)) and (len(pkt) < 9)

# Error packets are simpler to detect, because 0xFF is not (yet?)
# valid as the start of a length-encoded-integer.
proc isERRPacket*(pkt: string): bool = (len(pkt) >= 3) and (pkt[0] == char(ResponseCode_ERR))

proc isOKPacket*(pkt: string): bool = (len(pkt) >= 3) and (pkt[0] == char(ResponseCode_OK))

proc isAuthSwitchRequestPacket*(pkt: string): bool = 
  ## http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
  pkt[0] == char(ResponseCode_AuthSwitchRequest)

proc isExtraAuthDataPacket*(pkt: string): bool = 
  ## https://dev.mysql.com/doc/internals/en/successful-authentication.html
  pkt[0] == char(ResponseCode_ExtraAuthData)

proc parseErrorPacket*(pkt: string): ref ResponseERR =
  new(result)
  result.error_code = scanU16(pkt, 1)
  var pos: int
  if len(pkt) >= 9 and pkt[3] == '#':
    result.sqlstate = pkt.substr(4, 8)
    pos = 9
  else:
    pos = 3
  result.msg = pkt[pos .. high(pkt)]

proc parseAuthSwitchPacket*(conn: Connection, pkt: string): ref ResponseAuthSwitch =
  new(result)
  var pos: int = 1
  result.status = ResponseCode_ExtraAuthData
  result.pluginName = scanNulString(pkt, pos)
  result.pluginData = scanNulStringX(pkt, pos)

proc parseResponseAuthMorePacket*(conn: Connection,pkt: string): ref ResponseAuthMore =
  new(result)
  var pos: int = 1
  result.status = ResponseCode_ExtraAuthData
  result.pluginData = scanNulStringX(pkt, pos)

proc parseOKPacket*(conn: Connection, pkt: string): ResponseOK =
  result.eof = false
  var pos: int = 1
  result.affected_rows = scanLenInt(pkt, pos)
  result.last_insert_id = scanLenInt(pkt, pos)
  # We always supply Cap.protocol41 in client caps
  result.status_flags = cast[set[Status]]( scanU16(pkt, pos) )
  result.warning_count = scanU16(pkt, pos+2)
  pos = pos + 4
  if Cap.sessionTrack in conn.client_caps:
    result.info = scanLenStr(pkt, pos)
  else:
    result.info = scanNulStringX(pkt, pos)

proc parseEOFPacket*(pkt: string): ResponseOK =
  result.eof = true
  result.warning_count = scanU16(pkt, 1)
  result.status_flags = cast[set[Status]]( scanU16(pkt, 3) )

proc sendPacket*(conn: Connection, buf: var string, reset_seq_no = false): Future[void] =
  # Caller must have left the first four bytes of the buffer available for
  # us to write the packet header.
  let bodylen = len(buf) - 4
  buf[0] = char( (bodylen and 0xFF) )
  buf[1] = char( ((bodylen shr 8) and 0xFF) )
  buf[2] = char( ((bodylen shr 16) and 0xFF) )
  if reset_seq_no:
    conn.packet_number = 0
  buf[3] = char( conn.packet_number )
  inc(conn.packet_number)
  # hexdump(buf, stdmsg)
  conn.socket.send(buf)

proc writeHandshakeResponse*(conn: Connection,
                            username: string,
                            auth_response: string,
                            database: string,
                            auth_plugin: string): Future[void] =
  var buf: string = newStringOfCap(128)
  buf.setLen(4)

  var caps: set[Cap] = { Cap.longPassword, Cap.protocol41, Cap.secureConnection }
  if Cap.longFlag in conn.server_caps:
    incl(caps, Cap.longFlag)
  if auth_response.len > 0 and Cap.pluginAuthLenencClientData in conn.server_caps:
    if len(auth_response) > 255:
      incl(caps, Cap.pluginAuthLenencClientData)
  if database.len > 0 and Cap.connectWithDb in conn.server_caps:
    incl(caps, Cap.connectWithDb)
  if auth_plugin.len > 0:
    incl(caps, Cap.pluginAuth)

  conn.client_caps = caps

  # Fixed-length portion
  putU32(buf, cast[uint32](caps))
  putU32(buf, 65536'u32)  # max packet size, TODO: what should I put here?
  buf.add( char(Charset_utf8_ci) )

  # 23 bytes of filler
  for i in 1 .. 23:
    buf.add( char(0) )

  # Our username
  putNulString(buf, username)

  # Authentication data
  if auth_response.len > 0:
    if Cap.pluginAuthLenencClientData in caps:
      putLenInt(buf, len(auth_response))
      buf.add(auth_response)
    else:
      putU8(buf, len(auth_response))
      buf.add(auth_response)
  else:
    buf.add( char(0) )

  if Cap.connectWithDb in caps:
    putNulString(buf, database)

  if Cap.pluginAuth in caps:
    putNulString(buf, auth_plugin)

  return conn.sendPacket(buf)

proc sendQuery*(conn: Connection, query: string): Future[void] {.tags:[WriteIOEffect,RootEffect].} =
  var buf: string = newStringOfCap(4 + 1 + len(query))
  buf.setLen(4)
  buf.add( char(Command.query) )
  buf.add(query)
  return conn.sendPacket(buf, reset_seq_no=true)

## ######################################################################
##
## MySQL packet packers/unpackers

proc processHeader(c: Connection, hdr: array[4, char]): nat24 =
  result = int32(hdr[0]) + int32(hdr[1])*256 + int32(hdr[2])*65536
  let pnum = uint8(hdr[3])
  # stdmsg.writeLine("plen=", result, ", pnum=", pnum, " (expecting ", c.packet_number, ")")
  if pnum != c.packet_number:
    raise newException(ProtocolError, "Bad packet number (got sequence number " & $(pnum) & ", expected " & $(c.packet_number) & ")")
  c.packet_number += 1

when false:
  # Prototype synchronous code
  proc readExactly(s: Socket, buf: var openarray[char]) {.tags:[ReadIOEffect].} =
    var amount_read: int = 0
    while amount_read < len(buf):
      let r = s.recv(addr(buf[amount_read]), len(buf) - amount_read)
      if r < 0:
        socketError(s, r, false)
      if r == 0:
        raise newException(ProtocolError, "Connection closed")
      amount_read += r

  proc receivePacket*(conn: Connection): string {.tags:[ReadIOEffect].}=
    var b: array[4, char]
    readExactly(conn.socket, b)
    let packet_length = processHeader(conn, b)
    let pkt = newSeq[char](packet_length)
    conn.socket.readExactly(pkt)
    result = newString(len(pkt))
    # ugly, why are seq[char] and string so hard to interconvert?
    for i in 0 .. high(pkt):
      result[i] = pkt[i]

  proc send*(socket: Socket, data: openarray[char]): int {.tags:[WriteIOEffect].} =
    # This is horribly ugly, but it seems to be the only way to get
    # something from a seq into a socket
    let p = cast[ptr array[0 .. 1, char]](data)
    return socket.send(p, len(data))
else:
  proc receivePacket*(conn:Connection, drop_ok: bool = false): Future[string] {.async, tags:[ReadIOEffect,RootEffect].} =
    # drop_ok used when close
    let hdr = await conn.socket.recv(4)
    if len(hdr) == 0:
      if drop_ok:
        return ""
      else:
        raise newException(ProtocolError, "Connection closed")
    if len(hdr) != 4:
      raise newException(ProtocolError, "Connection closed unexpectedly")
    let b = cast[ptr array[4,char]](cstring(hdr))
    let packet_length = conn.processHeader(b[])
    if packet_length == 0:
      return ""
    result = await conn.socket.recv(packet_length)
    if len(result) == 0:
      raise newException(ProtocolError, "Connection closed unexpectedly")
    if len(result) != packet_length:
      raise newException(ProtocolError, "TODO finish this part")


proc roundtrip*(conn:Connection, data: string): Future[string] {.async, tags:[IOEffect,RootEffect].} =
  var buf: string = newStringOfCap(32)
  buf.setLen(4)
  buf.add data
  await conn.sendPacket(buf)
  let pkt = await conn.receivePacket()
  if isERRPacket(pkt):
    raise parseErrorPacket(pkt)
  return pkt
