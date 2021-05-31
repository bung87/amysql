
include ./protocol_basic
import ./cap
when defined(ChronosAsync):
  import chronos/[asyncloop, asyncsync, handles, transport, timer]
  import times except milliseconds,Duration,toParts,DurationZero,initDuration
  const DurationZero* = default(Duration)
  proc initDuration*(nanoseconds: int64=0, microseconds: int64 = 0, milliseconds: int64 = 0, seconds: int64 = 0, minutes: int64 = 0, hours: int64 = 0, days: int64 = 0, weeks: int64 = 0): Duration =
    default(Duration) + nanoseconds.nanoseconds + microseconds.microseconds + milliseconds.milliseconds + seconds.seconds + minutes.minutes + hours.hours + days.days + weeks.weeks
  proc toParts*(dur: Duration): DurationParts =
    
    var remS = dur.seconds
    var remNs = dur.nanoseconds.int

    # Ensure the same sign for seconds and nanoseconds
    if remS < 0 and remNs != 0:
      remNs -= convert(Seconds, Nanoseconds, 1)
      remS.inc 1

    for unit in countdown(Weeks, Seconds):
      let quantity = convert(Seconds, unit, remS)
      remS = remS mod convert(unit, Seconds, 1)

      result[unit] = quantity

    for unit in countdown(Milliseconds, Nanoseconds):
      let quantity = convert(Nanoseconds, unit, remNs)
      remNs = remNs mod convert(unit, Nanoseconds, 1)

      result[unit] = quantity
else:
  import asyncnet,asyncdispatch
  import times except milliseconds
import ../conn
import strutils
import net
import tables
import ./logger

const ReadTimeOut {.intdefine.} = 30_000
const WriteTimeOut {.intdefine.} = 60_000

when defined(mysql_compression_mode):
  # he default compression levels are initially set to 3 for zstd, 2 for LZ4, and 3 for Deflate. 
  const MinCompressLength {.intdefine.} = 50
  const ZstdCompressionLevel {.intdefine.} = 3
  import zstd

# EOF is signaled by a packet that starts with 0xFE, which is
# also a valid length-encoded-integer. In order to distinguish
# between the two cases, we check the length of the packet: EOFs
# are always short, and an 0xFE in a result row would be followed
# by at least 65538 bytes of data.
proc isEOFPacket*(conn:Connection): bool =
  let eofFlag = conn.firstByte.uint8 == ResponseCode_EOF
  result = eofFlag and conn.curPacketLen <= 9

proc isEOFPacketFollowed*(conn:Connection): bool =
  let eofFlag = conn.firstByte.uint8 == ResponseCode_EOF
  result = eofFlag and conn.remainPacketLen <= 9

proc isERRPacket*(conn:Connection): bool = conn.curPacketLen >= 7 and conn.firstByte.uint8 == ResponseCode_ERR

proc isOKPacket*(conn:Connection): bool = conn.curPacketLen >= 7 and conn.firstByte.uint8 == ResponseCode_OK

proc isAuthSwitchRequestPacket*(conn: Connection): bool = 
  ## http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
  conn.firstByte.uint8 == ResponseCode_AuthSwitchRequest

proc isExtraAuthDataPacket*(conn: Connection): bool = 
  ## https://dev.mysql.com/doc/internals/en/successful-authentication.html
  conn.firstByte.uint8 == ResponseCode_ExtraAuthData

proc isLocalInfileRequestPacket*(conn: Connection): bool =
  conn.firstByte.uint8 == ResponseCode_LOCAL_INFILE

proc parseLocalInfileRequestPacket*(conn: Connection): string =
  incPos conn
  result = cast[string](conn.buf[conn.bufPos .. ^1])

proc parseErrorPacket*(conn: Connection): ref ResponseERR =
  new(result)
  incPos conn # the error packet flag
  result.error_code = scanU16(conn.buf,conn.bufPos)
  incPos(conn,2)
  if conn.remainPacketLen >= 6 and conn.buf[conn.bufPos] == '#':
    # #HY000
    incPos(conn,6)
  else:
    incPos(conn,1)
  result.msg.setLen(conn.payloadLen - conn.bufPos + 4)
  copyMem(result.msg[0].addr,conn.buf[conn.bufPos].addr,conn.payloadLen - conn.bufPos + 4)

proc checkEof*(conn: Connection) {.inline.} =
  # int<1> header
  # if capabilities & CLIENT_PROTOCOL_41 {
  # int<2>	warnings	number of warnings
  # int<2>	status_flags	Status Flags
  # }
  if Cap.deprecateEof notin conn.clientCaps:
    conn.resetPacketLen
    if conn.firstByte.uint8 != ResponseCode_EOF:
      raise newException(ProtocolError, "Expected EOF after column defs, got something else fist byte:0x" & $conn.firstByte.uint8)
    else:
      if Cap.protocol41 in conn.clientCaps:
        incPos(conn,5)
      else:
        incPos conn

proc parseHandshakePacket*(conn: Connection): HandshakePacket = 
  ## https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
  new result
  result.protocolVersion = int(conn.firstByte)
  if result.protocolVersion != HandshakeV10.int:
    raise newException(ProtocolError, "Unexpected protocol version: " & $result.protocolVersion)
  incPos(conn)
  conn.serverVersion = readNulString(conn.buf, conn.bufPos)
  result.serverVersion = conn.serverVersion
  conn.threadId = scanU32(conn.buf, conn.bufPos)
  incPos(conn,4)
  result.threadId = int(conn.threadId)
  result.scramblebuff1 = newString(8)
  copyMem(result.scramblebuff1[0].addr,conn.buf[conn.bufPos].addr,8)
  incPos(conn,8)
  incPos conn # filter1
  result.capabilities1 = int(scanU16(conn.buf, conn.bufPos))
  incPos(conn,2)
  result.capabilities = result.capabilities1
  conn.serverCaps = cast[set[Cap]](result.capabilities1)
  result.charset = int(conn.buf[conn.bufPos])
  incPos conn
  result.serverStatus = int(scanU16(conn.buf, conn.bufPos))
  incPos conn,2
  result.protocol41 = (result.capabilities1 and Cap.protocol41.ord) > 0
  if not result.protocol41:
    raise newException(ProtocolError, "Old (pre-4.1) server protocol")
  result.capabilities2 = int(scanU16(conn.buf, conn.bufPos))
  incPos conn,2
  let cap = uint32(result.capabilities1) + (uint32(result.capabilities2) shl 16)
  conn.serverCaps = cast[set[Cap]]( cap )
  result.capabilities = int(cap)
  if Cap.pluginAuth in conn.serverCaps:
    result.scrambleLen = int(conn.buf[conn.bufPos])
  incPos conn
  incPos conn,10 # reserved
  if Cap.secureConnection in conn.serverCaps:
    let scrambleBuff2Len = max(13,result.scrambleLen - 8)
    result.scrambleBuff2 = newString(scrambleBuff2Len - 1) # null string
    copyMem(result.scrambleBuff2[0].addr,conn.buf[conn.bufPos].addr,scrambleBuff2Len - 1)
    incPos conn,scrambleBuff2Len
    result.scrambleBuff = result.scrambleBuff1 & result.scrambleBuff2
    assert result.scrambleBuff.len == 20
    assert result.scrambleLen == 21
  if Cap.pluginAuth in conn.serverCaps:
    result.plugin = readNulStringX(conn.buf, conn.bufPos)

proc parseAuthSwitchPacket*(conn: Connection): ref ResponseAuthSwitch =
  new(result)
  incPos(conn)
  result.status = ResponseCode_ExtraAuthData
  result.pluginName = readNulString(conn.buf, conn.bufPos)
  result.pluginData = readNulStringX(conn.buf, conn.bufPos)

proc parseResponseAuthMorePacket*(conn: Connection,pkt: string): ref ResponseAuthMore =
  new(result)
  incPos(conn)
  result.status = ResponseCode_ExtraAuthData
  result.pluginData = readNulStringX(conn.buf, conn.bufPos)

proc parseOKPacket*(conn: Connection): ResponseOK =
  # https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
  result.eof = conn.firstByte == char(ResponseCode_EOF)
  incPos(conn)
  result.affectedRows = readLenInt(conn.buf, conn.bufPos)
  result.lastInsertId = readLenInt(conn.buf, conn.bufPos)
  if Cap.protocol41 in conn.clientCaps:
    result.statusFlags = cast[set[Status]]( scanU16(conn.buf, conn.bufPos) )
    result.warningCount = scanU16(conn.buf, conn.bufPos+2)
    incPos(conn,4)
  elif Cap.transactions in conn.clientCaps:
    result.statusFlags = cast[set[Status]]( scanU16(conn.buf, conn.bufPos) )
    incPos(conn,2)
  conn.hasMoreResults = Status.moreResultsExist in result.statusFlags
  if Cap.sessionTrack in conn.clientCaps:
    result.info = readLenStr(conn.buf, conn.bufPos)
    if Status.sessionStateChanged in result.statusFlags:
      let sessionStateChangeDataLength = readLenInt(conn.buf, conn.bufPos)
      let endOffset = conn.bufPos + sessionStateChangeDataLength
      var typ:SessionStateType
      var name:string
      var value:string
      var dataLen:int
      while conn.bufPos < endOffset:
        typ = cast[SessionStateType](conn.buf[conn.bufPos])
        incPos conn
        dataLen = readLenInt(conn.buf, conn.bufPos)
        name = readLenStr(conn.buf, conn.bufPos)
        if typ == SessionStateType.systemVariables:
          value = readLenStr(conn.buf, conn.bufPos)
        result.sessionStateChanges.add SessionState(typ:typ,name:name,value:value)
        value = ""
  else:
    result.info = readNulStringX(conn.buf, conn.bufPos)
  
proc parseEOFPacket*(conn: Connection): ResponseOK =
  result.eof = true
  incPos conn
  if Cap.protocol41 in conn.clientCaps:
    result.warningCount = scanU16(conn.buf, conn.bufPos)
    incPos(conn,2)
    result.statusFlags = cast[set[Status]]( scanU16(conn.buf, conn.bufPos) )
    incPos(conn,2)

proc sendPacket*(conn: Connection, buf: sink string, resetSeqId = false): Future[void] {.async.} =
  # Caller must have left the first four bytes of the buffer available for
  # us to write the packet header.
  # https://dev.mysql.com/doc/internals/en/compressed-packet-header.html
  when TestWhileIdle:
    when not defined(ChronosAsync):
      conn.lastOperationTime = now()
    else:
      conn.lastOperationTime = Moment.now()
  const TimeoutErrorMsg = "Timeout when send packet"
  let bodyLen = len(buf) - 4
  setInt32(buf,0,bodyLen)
  if resetSeqId:
    conn.sequenceId = 0
    when defined(mysql_compression_mode):
      conn.compressedSequenceId = 0
  when not defined(mysql_compression_mode):
    buf[3] = char( conn.sequenceId )
    inc(conn.sequenceId)
    var success = true
    when defined(ChronosAsync):
      let send = conn.transp.write(buf[0].addr,buf.len)
      try:
        discard await wait(send, WriteTimeOut)
      except AsyncTimeoutError:
        success = false
    else:
      let send = conn.transp.send(buf,flags = {})
      success = await withTimeout(send, WriteTimeOut)
    if not success:
      raise newException(TimeoutError, TimeoutErrorMsg)
  else:
    # set global protocol_compression_algorithms='zstd,uncompressed';
    # default value: zlib,zstd,uncompressed
    if conn.use_zstd():
      var packet:seq[char]
      var compressed:seq[byte]
      var packetLen:int
      let bufLen = bodyLen + 4
      if bodyLen >= MinCompressLength:
        # https://dev.mysql.com/doc/internals/en/compressed-packet-header.html
        # https://dev.mysql.com/doc/internals/en/example-one-mysql-packet.html
        compressed = compress(cast[ptr UncheckedArray[byte]](buf[0].addr).toOpenArray(0,buf.high),ZstdCompressionLevel)
        let compressedLen = compressed.len
        packetLen = 7 + compressedLen
        packet = newSeqOfCap[char](packetLen)
        packet.setLen(7)
        setInt32(packet,0,compressedLen)
        setInt32(packet,4,bufLen)
        debug "bodyLen >= MinCompressLength"
      else:
        # https://dev.mysql.com/doc/internals/en/uncompressed-payload.html
        debug "bodyLen < MinCompressLength"
        let bufLen = bodyLen + 4
        packetLen = 7 + bufLen
        packet = newSeqOfCap[char](packetLen)
        packet.setLen(7)
        setInt32(packet,0,bufLen)
        setInt32(packet,4,0)
      packet[3] = char( conn.compressedSequenceId )
      inc(conn.compressedSequenceId)
      if bodyLen >= MinCompressLength:
        packet.add cast[ptr UncheckedArray[char]](compressed[0].addr).toOpenArray(0,compressed.high)
      else:
        packet.add buf
      var success = true
      when not defined(ChronosAsync):
        let send = conn.transp.send(packet[0].addr,packetLen)
        success = await withTimeout(send, WriteTimeOut)
      else:
        let send = conn.transp.write(packet[0].addr,packetLen)
        try:
          discard await wait(send, WriteTimeOut)
        except AsyncTimeoutError:
          success = false
      if not success:
        raise newException(TimeoutError, TimeoutErrorMsg)
    else:
      buf[3] = char( conn.sequenceId )
      inc(conn.sequenceId)
      var success = true
      when not defined(ChronosAsync):
        let send = conn.transp.send(buf)
        success = await withTimeout(send, WriteTimeOut)
      else:
        let send = conn.transp.write(buf)
        try:
          discard await wait(send, WriteTimeOut)
        except AsyncTimeoutError:
          success = false
      if not success:
        raise newException(TimeoutError, TimeoutErrorMsg)

proc writeHandshakeResponse*(conn: Connection,
                            username: string,
                            auth_response: string,
                            database: string,
                            auth_plugin: string): Future[void] {.async.} =
  # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html
  var buf: string = newStringOfCap(128)
  buf.setLen(4)

  var caps: set[Cap] = BasicClientCaps
  if Cap.longFlag in conn.serverCaps:
    incl(caps, Cap.longFlag)
  if auth_response.len > 0 and Cap.pluginAuthLenencClientData in conn.serverCaps:
    if len(auth_response) > 255:
      incl(caps, Cap.pluginAuthLenencClientData)
  if database.len > 0 and Cap.connectWithDb in conn.serverCaps:
    incl(caps, Cap.connectWithDb)
  if auth_plugin.len > 0:
    incl(caps, Cap.pluginAuth)
  let connectAttrsLen = len(conn.connectAttrs)
  if connectAttrsLen > 0 and  Cap.connectAttrs in conn.serverCaps:
    incl(caps, Cap.connectAttrs)
  if Cap.deprecateEof in conn.serverCaps:
    incl(caps, Cap.deprecateEof)
  if Cap.localFiles in conn.serverCaps:
    incl(caps, Cap.localFiles)
  if Cap.sessionTrack in conn.serverCaps:
    incl(caps, Cap.sessionTrack)
  if Cap.multiStatements in conn.serverCaps:
    incl(caps, Cap.multiStatements)
  if Cap.multiResults in conn.serverCaps:
    incl(caps, Cap.multiResults)
  when defined(mysql_compression_mode):
    if Cap.zstdCompressionAlgorithm in conn.serverCaps:
      incl(caps, Cap.zstdCompressionAlgorithm)

  conn.clientCaps = caps

  # Fixed-length portion
  putU32(buf, cast[uint32](caps))
  # https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet
  putU32(buf, 65536'u32)  # max packet size
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
    elif Cap.secureConnection in caps:
      putU8(buf, len(auth_response))
      buf.add(auth_response)
    else:
      putNulString(buf, auth_response)
  else:
    buf.add( char(0) )

  if Cap.connectWithDb in caps:
    putNulString(buf, database)

  if Cap.pluginAuth in caps:
    putNulString(buf, auth_plugin)
  if Cap.connectAttrs in caps:
    # https://dev.mysql.com/doc/refman/5.6/en/performance-schema-connection-attribute-tables.html#performance-schema-connection-attributes-available
    # https://dev.mysql.com/doc/refman/5.6/en/performance-schema-session-connect-attrs-table.html
    # Attribute names that begin with an underscore (_) are reserved for internal use and should not be created by application programs. 
    var count = 0
    var kLen = 0
    var vLen = 0
    for k,v in conn.connectAttrs.mpairs:
      kLen = k.len
      vLen = v.len
      inc count,kLen
      inc count,vLen
      inc count,countLenInt(kLen)
      inc count,countLenInt(vLen)
    putLenInt(buf, count)
    for k,v in conn.connectAttrs.mpairs:
      putLenStr(buf, k)
      putLenStr(buf, v)
  when defined(mysql_compression_mode):
    if Cap.zstdCompressionAlgorithm in caps:
      # For zlib compression method, the default compression level will be set to 6
      # and for zstd it is 3. Valid compression levels for zstd is between 1 to 22 
      # inclusive.
      putU8(buf, ZstdCompressionLevel)

  await conn.sendPacket(buf)

proc putTime*(buf: var string, val: Duration):int {.discardable.}  =
  let dp = toParts(val)
  var micro = dp[Microseconds].int32
  result = if micro == 0: 8 else: 12
  buf.putU8(result) # length
  buf.putU8(if val < DurationZero: 1 else: 0 ) 
  var days = dp[Days].int32
  buf.put32 days.addr
  buf.putU8 dp[Hours]
  buf.putU8 dp[Minutes]
  buf.putU8 dp[Seconds]
  if micro != 0:
    buf.put32 micro.addr

proc readTime*(buf: openarray[char], pos: var int): Duration = 
  let dataLen = int(buf[pos])
  var isNegative = int(buf[pos + 1])
  inc(pos,2)
  var days:int32
  scan32(buf,pos,days.addr)
  inc(pos,4)
  var hours = int(buf[pos])
  var minutes = int(buf[pos + 1])
  var seconds = int(buf[pos + 2])
  inc(pos,3)
  var microseconds:int32 
  if dataLen == 8 :
    microseconds = 0 
  else: 
    scan32(buf,pos,microseconds.addr)
    inc(pos,4)
  if isNegative != 0:
    days = -days
    hours = -hours
    minutes = -minutes
    seconds = -seconds
    microseconds = -microseconds
  initDuration(days=days,hours=hours,minutes=minutes,seconds=seconds,microseconds=microseconds)

proc putDate*(buf: var string, val: DateTime):int {.discardable.}  =
  result = 4
  buf.putU8 result.uint8
  var uyear = val.year.uint16
  buf.put16 uyear.addr
  buf.putU8 val.month.ord.uint8
  buf.putU8 val.monthday.uint8

proc putDateTime*(buf: var string, val: DateTime):int {.discardable.} =
  if default(DateTime) == val:
    result = 0
    buf.putU8 0.uint8
    return result
  let hasTime = val.second != 0 or val.minute != 0 or val.hour != 0
  if val.nanosecond != 0:
    result = 11
  elif hasTime:
    result = 7
  else:
    result = 4
  buf.putU8 result.uint8 # length
  var uyear = val.year.uint16
  buf.putU16 uyear
  buf.putU8 val.month.ord.uint8
  buf.putU8 val.monthday.uint8
  
  if result > 4:
    buf.putU8 val.hour.uint8
    buf.putU8 val.minute.uint8
    buf.putU8 val.second.uint8
    if result > 7:
      var micro = val.nanosecond div 1000
      var umico = micro.int32
      buf.put32 umico.addr

proc readDateTime*(buf: openarray[char], pos: var int, zone: Timezone = utc()): DateTime = 
  let year = int(buf[pos+1]) + int(buf[pos+2]) * 256
  inc(pos,2)
  let month = int(buf[pos + 1])
  let day = int(buf[pos + 2])
  inc(pos,2)
  var hour,minute,second:int
  hour = int(buf[pos + 1])
  minute = int(buf[pos + 2])
  second = int(buf[pos + 3])
  inc(pos,3)
  if year == 0 and month == 0 and day == 0:
    return default(DateTime)
  result = initDateTime(day,month.Month,year.int,hour,minute,second,zone)

proc putTimestamp*(buf: var string, val: DateTime): int {.discardable.} = 
  # for text protocol
  let ts = val.format("yyyy-MM-dd HH:mm:ss'.'ffffff") # len 26 + 13
  buf.putNulString "timestamp('$#')" % [ts]
  # default "timestamp('0000-00-00')" len 23

proc hexdump*(buf: openarray[char], fp: File) =
  var pos = low(buf)
  while pos <= high(buf):
    for i in 0 .. 15:
      fp.write(' ')
      if i == 8: fp.write(' ')
      let p = i+pos
      fp.write( if p <= high(buf): toHex(int(buf[p]), 2) else: "  " )
    fp.write("  |")
    for i in 0 .. 15:
      var ch = ( if (i+pos) > high(buf): ' ' else: buf[i+pos] )
      if ch < ' ' or ch > '~':
        ch = '.'
      fp.write(ch)
    pos += 16
    fp.write("|\n")

proc sendQuery*(conn: Connection, query: string): Future[void] {.tags:[WriteIOEffect,RootEffect].} =
  var buf: string = newStringOfCap(4 + 1 + len(query))
  buf.setLen(4)
  buf.add( char(Command.query) )
  buf.add(query)
  return conn.sendPacket(buf, resetSeqId=true)

proc sendFile*(conn: Connection, filename: string): Future[void] {.tags:[WriteIOEffect,RootEffect].} =
  let content = readFile(filename)
  var buf: string = newStringOfCap(4 + len(content))
  buf.setLen(4)
  buf.add(content)
  return conn.sendPacket(buf, resetSeqId=false)

proc sendEmptyPacket*(conn: Connection): Future[void] {.tags:[WriteIOEffect,RootEffect].} =
  var buf: string = newStringOfCap(4)
  buf.setLen(4)
  return conn.sendPacket(buf, resetSeqId=false)

## MySQL packet packers/unpackers

proc processHeader(c: Connection): nat24 =
  result = c.getPayloadLen
  const errMsg = "Bad packet id (got sequence id $1, expected $2)"
  const errMsg2 = "Bad packet id (got compressed sequence id $1, expected $2)"
  let id = uint8(c.buf[3])
  when defined(mysql_compression_mode):
    if c.use_zstd():
      # if id != c.compressedSequenceId:
      #   raise newException(ProtocolError, errMsg2.format(id,c.compressedSequenceId ) )
      c.compressedSequenceId += 1
    else:
      # if id != c.sequenceId:
      #   raise newException(ProtocolError, errMsg.format(id,c.sequenceId) )
      c.sequenceId += 1
  else:
    # if id != c.sequenceId:
    #   raise newException(ProtocolError, errMsg.format(id,c.sequenceId) )
    c.sequenceId += 1

proc receivePacket*(conn:Connection, drop_ok: bool = false) {.async, tags:[ReadIOEffect,RootEffect].} =
  # drop_ok used when close
  # https://dev.mysql.com/doc/internals/en/uncompressed-payload.html
  conn.zeroPos()
  if conn.buf.len > MysqlBufSize:
    conn.buf.setLen(MysqlBufSize)
  zeroMem conn.buf[0].addr,MysqlBufSize
  when TestWhileIdle:
    when not defined(ChronosAsync):
      conn.lastOperationTime = now()
    else:
      conn.lastOperationTime = Moment.now()
  const TimeoutErrorMsg = "Timeout when receive packet"
  const NormalLen = 4
  const CompressedLen = 7
  var offset:int
  var headerLen:int
  var uncompressedLen:int32
  when not defined(mysql_compression_mode):
    offset = NormalLen
    var success = true
    when not defined(ChronosAsync):
      let rec = conn.transp.recvInto(conn.buf[0].addr, NormalLen,flags = {})
      success = await withTimeout(rec, ReadTimeOut)
    else:
      let rec = conn.transp.readOnce(conn.buf[0].addr, NormalLen)
      try:
        discard await wait(rec, ReadTimeOut)
      except AsyncTimeoutError:
        success = false
    if not success:
      raise newException(TimeoutError, TimeoutErrorMsg)
    headerLen = rec.read
  else:
    if conn.use_zstd():
      # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_compression_packet.html#sect_protocol_basic_compression_packet_header
      # raw packet length                      -> 41
      # (3)compressed payload length   = 22 00 00 -> 34 (41 - 7)
      # (1)sequence id                 = 00       ->  0
      # (3)uncompressed payload length = 32 00 00 -> 50
      offset = CompressedLen
      var success = true
      when not defined(ChronosAsync):
        let rec = conn.transp.recvInto(conn.buf[0].addr,CompressedLen)
        success = await withTimeout(rec, ReadTimeOut)
      else:
        let rec = conn.transp.readOnce(conn.buf[0].addr,CompressedLen)
        try:
          discard await wait(rec, ReadTimeOut)
        except AsyncTimeoutError:
          success = false
      if not success:
        raise newException(TimeoutError, TimeoutErrorMsg)
      headerLen = rec.read

      uncompressedLen = int32( uint32(conn.buf[conn.bufPos + 4]) or (uint32(conn.buf[conn.bufPos+5]) shl 8) or (uint32(conn.buf[conn.bufPos+6]) shl 16) )
    else:
      offset = NormalLen
      var success = true
      when not defined(ChronosAsync):
        let rec = conn.transp.recvInto(conn.buf[0].addr,NormalLen)
        success = await withTimeout(rec, ReadTimeOut)
      else:
        let rec = conn.transp.readOnce(conn.buf[0].addr,NormalLen)
        try:
          discard await wait(rec, ReadTimeOut)
        except AsyncTimeoutError:
          success = false
      if not success:
        raise newException(TimeoutError, TimeoutErrorMsg)
      headerLen = rec.read
  if headerLen == 0:
    if drop_ok:
      return 
    else:
      raise newException(ProtocolError, "Connection closed")
  if headerLen != 4 and headerLen != 7:
    raise newException(ProtocolError, "Connection closed unexpectedly")
  conn.payloadLen = conn.processHeader()
  conn.fullPacketLen = conn.payloadLen + offset
  conn.curPacketLen = conn.fullPacketLen
  # conn.remainPacketLen = conn.fullPacketLen
  
  if conn.payloadLen == 0:
    return 
  if conn.fullPacketLen > MysqlBufSize:
    conn.buf.setLen(offset + conn.payloadLen)
  var payloadRecvSuccess = true
  when not defined(ChronosAsync):
    let payload = conn.transp.recvInto(conn.buf[offset].addr,conn.payloadLen)
    payloadRecvSuccess = await withTimeout(payload, ReadTimeOut)
  else:
    let payload = conn.transp.readOnce(conn.buf[offset].addr,conn.payloadLen)
    try:
      discard await wait(payload, ReadTimeOut)
    except AsyncTimeoutError:
      payloadRecvSuccess = false
  if not payloadRecvSuccess:
    raise newException(TimeoutError, TimeoutErrorMsg)
  conn.bufLen = payload.read
  if conn.bufLen == 0:
    raise newException(ProtocolError, "Connection closed unexpectedly")
  if conn.bufLen != conn.payloadLen:
    raise newException(ProtocolError, "TODO finish this part")
  when defined(mysql_compression_mode):
    if conn.use_zstd():
      conn.incPos offset
      let isUncompressed = uncompressedLen == 0
      if isUncompressed:
        # 07 00 00 02  00                      00                          00                02   00            00 00
        # header(4)    affected rows(lenenc)   lastInsertId(lenenc)     AUTOCOMMIT enabled statusFlags(2)    warnning(2)
        debug "result is uncompressed" 
      else:
        if offset + uncompressedLen  > MysqlBufSize:
          conn.buf.setLen(offset + uncompressedLen)
        for i,c in decompress(cast[ptr UnCheckedArray[byte]](conn.buf[offset].addr).toOpenArray(0,conn.payloadLen - 1)):
          conn.buf[offset + i] = char(c)
        conn.payloadLen = uncompressedLen - 4
        debug "result is compressed" 
      # conn.resetPacketLen

proc roundtrip*(conn:Connection, data:sink string):Future[void] {.async, tags:[IOEffect,RootEffect].} =
  var buf: string = newStringOfCap(32)
  buf.setLen(4)
  buf.add data
  await conn.sendPacket(buf)
  await conn.receivePacket()
  if isERRPacket(conn):
    raise parseErrorPacket(conn)
  return

proc processMetadata*(conn: Connection, meta:var seq[ColumnDefinition], index: int) =
  # https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41
  # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
  meta[index].catalog = readLenStr(conn.buf, conn.bufPos)
  meta[index].schema = readLenStr(conn.buf, conn.bufPos)
  meta[index].table = readLenStr(conn.buf, conn.bufPos)
  meta[index].origTable = readLenStr(conn.buf, conn.bufPos)
  meta[index].name = readLenStr(conn.buf, conn.bufPos)
  meta[index].origName = readLenStr(conn.buf, conn.bufPos)
  let extras_len = readLenInt(conn.buf, conn.bufPos)
  # length of the following fields (always 0x0c)
  # if extras_len < 10 or (conn.bufPos+extras_len > len(conn.buf)):
  #   raise newException(ProtocolError, "truncated column packet")
  meta[index].charset = int16(scanU16(conn.buf, conn.bufPos))
  incPos conn,2
  meta[index].length = scanU32(conn.buf, conn.bufPos)
  incPos conn,4
  meta[index].columnType = FieldType(uint8(conn.buf[conn.bufPos]))
  incPos conn,1
  meta[index].flags = cast[set[FieldFlag]](scanU16(conn.buf, conn.bufPos))
  incPos conn,2
  meta[index].decimals = int(conn.buf[conn.bufPos])
  incPos conn
  incPos(conn, 2) # filter internals manual mentioned

proc receiveMetadata*(conn: Connection, count: Positive): Future[seq[ColumnDefinition]] {.async.} =
  var received = 0
  result = newSeq[ColumnDefinition](count)
  while received < count:
    await conn.receivePacket()
    conn.resetPacketLen
    if conn.firstByte.uint8 == ResponseCode_ERR or conn.firstByte.uint8 == ResponseCode_EOF:
      raise newException(ProtocolError, "Receive $1 when receiveMetadata" % [$conn.firstByte.uint8])
      # break
    conn.processMetadata(result,received)
    inc(received)
  if Cap.deprecateEof notin conn.clientCaps:
    await conn.receivePacket()
    conn.resetPacketLen
    if conn.firstByte.uint8 != ResponseCode_EOF:
      raise newException(ProtocolError, "Expected EOF after column defs, got something else fist byte:0x" & $conn.firstByte.uint8)