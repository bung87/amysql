
include protocol_basic

import asyncnet,asyncdispatch


# These are protocol constants; see
#  https://dev.mysql.com/doc/internals/en/overview.html

const
  ResponseCode_OK  : uint8 = 0
  ResponseCode_EOF : uint8 = 254   # Deprecated in mysql 5.7.5
  ResponseCode_ERR : uint8 = 255

  NullColumn       = char(0xFB)

  HandshakeV10 : uint8 = 0x0A  # Initial handshake packet since MySQL 3.21

  Charset_swedish_ci : uint8 = 0x08
  Charset_utf8_ci    : uint8 = 0x21
  Charset_binary     : uint8 = 0x3f



type
  # These correspond to the bits in the capability words,
  # and the CLIENT_FOO_BAR definitions in mysql. We rely on
  # Nim's set representation being compatible with the
  # C bit-masking convention.
  Cap {.pure.} = enum
    longPassword = 0 # new more secure passwords
    foundRows = 1 # Found instead of affected rows
    longFlag = 2 # Get all column flags
    connectWithDb = 3 # One can specify db on connect
    noSchema = 4 # Don't allow database.table.column
    compress = 5 # Can use compression protocol
    odbc = 6 # Odbc client
    localFiles = 7 # Can use LOAD DATA LOCAL
    ignoreSpace = 8 # Ignore spaces before '('
    protocol41 = 9 # New 4.1 protocol
    interactive = 10 # This is an interactive client
    ssl = 11 # Switch to SSL after handshake
    ignoreSigpipe = 12  # IGNORE sigpipes
    transactions = 13 # Client knows about transactions
    reserved = 14  # Old flag for 4.1 protocol
    secureConnection = 15  # Old flag for 4.1 authentication
    multiStatements = 16  # Enable/disable multi-stmt support
    multiResults = 17  # Enable/disable multi-results
    psMultiResults = 18  # Multi-results in PS-protocol
    pluginAuth = 19  # Client supports plugin authentication
    connectAttrs = 20  # Client supports connection attributes
    pluginAuthLenencClientData = 21  # Enable authentication response packet to be larger than 255 bytes.
    canHandleExpiredPasswords = 22  # Don't close the connection for a connection with expired password.
    sessionTrack = 23
    deprecateEof = 24  # Client no longer needs EOF packet
    sslVerifyServerCert = 30
    rememberOptions = 31

  nat24 = range[0 .. 16777215]
  Connection* = ref ConnectionObj
  ConnectionObj = object of RootObj
    socket: AsyncSocket               # Bytestream connection
    packet_number: uint8              # Next expected seq number (mod-256)

    # Information from the connection setup
    server_version*: string
    thread_id*: uint32
    server_caps: set[Cap]

    # Other connection parameters
    client_caps: set[Cap]

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
  Command {.pure.} = enum
    # sleep = 0
    quiT = 1
    initDb = 2
    query = 3
    fieldList = 4
    createDb = 5
    dropDb = 6
    refresh = 7
    shutdown = 8
    statistics = 9
    processInfo = 10
    # connect = 11
    processKill = 12
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

type
  greetingVars {.final.} = object
    scramble: string
    authentication_plugin: string


  # Server response packets: OK and EOF
  ResponseOK = object {.final.}
    eof               : bool  # True if EOF packet, false if OK packet
    affected_rows*    : Natural
    last_insert_id*   : Natural
    status_flags*     : set[Status]
    warning_count*    : Natural
    info*             : string
    # session_state_changes: seq[ ... ]

  # Server response packet: ERR (which can be thrown as an exception)
  ResponseERR = object of CatchableError
    error_code: uint16
    sqlstate: string
# EOF is signaled by a packet that starts with 0xFE, which is
# also a valid length-encoded-integer. In order to distinguish
# between the two cases, we check the length of the packet: EOFs
# are always short, and an 0xFE in a result row would be followed
# by at least 65538 bytes of data.
proc isEOFPacket(pkt: string): bool =
  result = (len(pkt) >= 1) and (pkt[0] == char(ResponseCode_EOF)) and (len(pkt) < 9)

# Error packets are simpler to detect, because 0xFF is not (yet?)
# valid as the start of a length-encoded-integer.
proc isERRPacket(pkt: string): bool = (len(pkt) >= 3) and (pkt[0] == char(ResponseCode_ERR))

proc isOKPacket(pkt: string): bool = (len(pkt) >= 3) and (pkt[0] == char(ResponseCode_OK))

proc parseErrorPacket(pkt: string): ref ResponseERR =
  new(result)
  result.error_code = scanU16(pkt, 1)
  var pos: int
  if len(pkt) >= 9 and pkt[3] == '#':
    result.sqlstate = pkt.substr(4, 8)
    pos = 9
  else:
    pos = 3
  result.msg = pkt[pos .. high(pkt)]

proc parseOKPacket(conn: Connection, pkt: string): ResponseOK =
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

proc parseEOFPacket(pkt: string): ResponseOK =
  result.eof = true
  result.warning_count = scanU16(pkt, 1)
  result.status_flags = cast[set[Status]]( scanU16(pkt, 3) )

proc parseInitialGreeting(conn: Connection, greeting: string): greetingVars =
  let protocolVersion = uint8(greeting[0])
  if protocolVersion != HandshakeV10:
    raise newException(ProtocolError, "Unexpected protocol version: 0x" & toHex(int(protocolVersion), 2))
  var pos = 1
  conn.server_version = scanNulString(greeting, pos)
  conn.thread_id = scanU32(greeting, pos)
  pos += 4
  result.scramble = greeting[pos .. pos+7]
  let cflags_l = scanU16(greeting, pos + 8 + 1)
  conn.server_caps = cast[set[Cap]](cflags_l)
  pos += 11

  if not (Cap.protocol41 in conn.server_caps):
    raise newException(ProtocolError, "Old (pre-4.1) server protocol")

  if len(greeting) >= (pos+5):
    let cflags_h = scanU16(greeting, pos+3)
    conn.server_caps = cast[set[Cap]]( uint32(cflags_l) + (uint32(cflags_h) shl 16) )

    let moreScram = ( if Cap.protocol41 in conn.server_caps: int(greeting[pos+5]) else: 0 )
    if moreScram > 8:
      result.scramble.add(greeting[pos + 16 .. pos + 16 + moreScram - 8 - 2])
    pos = pos + 16 + ( if moreScram < 20: 12 else: moreScram - 8 )

    if Cap.pluginAuth in conn.server_caps:
      result.authentication_plugin = scanNulStringX(greeting, pos)

# Caller must have left the first four bytes of the buffer available for
# us to write the packet header.
proc sendPacket(conn: Connection, buf: var string, reset_seq_no = false): Future[void] =
  let bodylen = len(buf) - 4
  buf[0] = char( (bodylen and 0xFF) )
  buf[1] = char( ((bodylen shr 8) and 0xFF) )
  buf[2] = char( ((bodylen shr 16) and 0xFF) )
  if reset_seq_no:
    conn.packet_number = 0
  buf[3] = char( conn.packet_number )
  inc(conn.packet_number)
  # hexdump(buf, stdmsg)
  return conn.socket.send(buf)

proc writeHandshakeResponse(conn: Connection,
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

proc sendQuery(conn: Connection, query: string): Future[void] =
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
  proc readExactly(s: Socket, buf: var openarray[char]) =
    var amount_read: int = 0
    while amount_read < len(buf):
      let r = s.recv(addr(buf[amount_read]), len(buf) - amount_read)
      if r < 0:
        socketError(s, r, false)
      if r == 0:
        raise newException(ProtocolError, "Connection closed")
      amount_read += r

  proc receivePacket(conn: Connection): string =
    var b: array[4, char]
    readExactly(conn.socket, b)
    let packet_length = processHeader(conn, b)
    let pkt = newSeq[char](packet_length)
    conn.socket.readExactly(pkt)
    result = newString(len(pkt))
    # ugly, why are seq[char] and string so hard to interconvert?
    for i in 0 .. high(pkt):
      result[i] = pkt[i]

  proc send(socket: Socket, data: openarray[char]): int =
    # This is horribly ugly, but it seems to be the only way to get
    # something from a seq into a socket
    let p = cast[ptr array[0 .. 1, char]](data)
    return socket.send(p, len(data))
else:
  proc receivePacket(conn:Connection, drop_ok: bool = false): Future[string] {.async.} =
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