
include ./protocol_basic
import ./cap
import asyncnet,asyncdispatch
import ../conn
import times
import strutils
import net
import logging

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
  result = conn.curPayloadLen >= 3 and conn.firstByte == char(ResponseCode_EOF) and conn.curPayloadLen < 9

# Error packets are simpler to detect, because 0xFF is not (yet?)
# valid as the start of a length-encoded-integer.
proc isERRPacket*(conn:Connection): bool = conn.curPayloadLen >= 3 and conn.firstByte == char(ResponseCode_ERR)

proc isOKPacket*(conn:Connection): bool = conn.curPayloadLen >= 3 and conn.firstByte == char(ResponseCode_OK)

proc isAuthSwitchRequestPacket*(conn: Connection): bool = 
  ## http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
  conn.firstByte == char(ResponseCode_AuthSwitchRequest)

proc isExtraAuthDataPacket*(conn: Connection): bool = 
  ## https://dev.mysql.com/doc/internals/en/successful-authentication.html
  conn.firstByte == char(ResponseCode_ExtraAuthData)

proc parseErrorPacket*(conn: Connection): ref ResponseERR =
  new(result)
  inc conn.bufPos # the error packet flag
  result.error_code = scanU16(conn.buf,conn.bufPos)
  inc(conn.bufPos,2)
  if conn.curPayloadLen >= 9 and conn.buf[conn.bufPos] == '#':
    # #HY000
    inc(conn.bufPos,6)
  else:
    inc(conn.bufPos,1)
  result.msg.setLen(conn.payloadLen - conn.bufPos)
  copyMem(result.msg[0].addr,conn.buf[conn.bufPos].addr,conn.payloadLen - conn.bufPos)

proc parseHandshakePacket*(conn: Connection): HandshakePacket = 
  ## https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
  new result
  result.protocolVersion = int(conn.firstByte)
  if result.protocolVersion != HandshakeV10.int:
    raise newException(ProtocolError, "Unexpected protocol version: " & $result.protocolVersion)
  inc(conn.bufPos)
  conn.serverVersion = readNulString(conn.buf, conn.bufPos)
  result.serverVersion = conn.serverVersion
  conn.threadId = scanU32(conn.buf, conn.bufPos)
  inc(conn.bufPos,4)
  result.threadId = int(conn.threadId)
  result.scramblebuff1 = newString(8)
  copyMem(result.scramblebuff1[0].addr,conn.buf[conn.bufPos].addr,8)
  inc(conn.bufPos,8)
  inc conn.bufPos # filter1
  result.capabilities1 = int(scanU16(conn.buf, conn.bufPos))
  inc(conn.bufPos,2)
  result.capabilities = result.capabilities1
  conn.serverCaps = cast[set[Cap]](result.capabilities1)
  result.charset = int(conn.buf[conn.bufPos])
  inc conn.bufPos
  result.serverStatus = int(scanU16(conn.buf, conn.bufPos))
  inc conn.bufPos,2
  result.protocol41 = (result.capabilities1 and Cap.protocol41.ord) > 0
  if not result.protocol41:
    raise newException(ProtocolError, "Old (pre-4.1) server protocol")
  result.capabilities2 = int(scanU16(conn.buf, conn.bufPos))
  inc conn.bufPos,2
  let cap = uint32(result.capabilities1) + (uint32(result.capabilities2) shl 16)
  conn.serverCaps = cast[set[Cap]]( cap )
  result.capabilities = int(cap)
  if Cap.pluginAuth in conn.serverCaps:
    result.scrambleLen = int(conn.buf[conn.bufPos])
  inc conn.bufPos
  inc conn.bufPos,10 # reserved
  if Cap.secureConnection in conn.serverCaps:
    let scrambleBuff2Len = max(13,result.scrambleLen - 8)
    result.scrambleBuff2 = newString(scrambleBuff2Len - 1) # null string
    copyMem(result.scrambleBuff2[0].addr,conn.buf[conn.bufPos].addr,scrambleBuff2Len - 1)
    inc conn.bufPos,scrambleBuff2Len
    result.scrambleBuff = result.scrambleBuff1 & result.scrambleBuff2
    assert result.scrambleBuff.len == 20
    assert result.scrambleLen == 21
  if Cap.pluginAuth in conn.serverCaps:
    result.plugin = readNulStringX(conn.buf, conn.bufPos)

proc parseAuthSwitchPacket*(conn: Connection): ref ResponseAuthSwitch =
  new(result)
  inc(conn.bufPos)
  result.status = ResponseCode_ExtraAuthData
  result.pluginName = readNulString(conn.buf, conn.bufPos)
  result.pluginData = readNulStringX(conn.buf, conn.bufPos)

proc parseResponseAuthMorePacket*(conn: Connection,pkt: string): ref ResponseAuthMore =
  new(result)
  inc(conn.bufPos)
  result.status = ResponseCode_ExtraAuthData
  result.pluginData = readNulStringX(conn.buf, conn.bufPos)

proc parseOKPacket*(conn: Connection): ResponseOK =
  result.eof = false
  inc(conn.bufPos)
  result.affectedRows = readLenInt(conn.buf, conn.bufPos)
  result.lastInsertId = readLenInt(conn.buf, conn.bufPos)
  # We always supply Cap.protocol41 in client caps
  result.statusFlags = cast[set[Status]]( scanU16(conn.buf, conn.bufPos) )
  result.warningCount = scanU16(conn.buf, conn.bufPos+2)
  inc(conn.bufPos,4)
  if Cap.sessionTrack in conn.clientCaps:
    result.info = readLenStr(conn.buf, conn.bufPos)
  else:
    result.info = readNulStringX(conn.buf, conn.bufPos)

proc parseEOFPacket*(conn: Connection): ResponseOK =
  result.eof = true
  inc conn.bufPos
  if conn.curPayloadLen > 1:
    result.warningCount = scanU16(conn.buf, conn.bufPos)
    inc(conn.bufPos,2)
    result.statusFlags = cast[set[Status]]( scanU16(conn.buf, conn.bufPos) )
    inc(conn.bufPos,2)

proc sendPacket*(conn: Connection, buf: sink string, resetSeqId = false): Future[void] {.async.} =
  # Caller must have left the first four bytes of the buffer available for
  # us to write the packet header.
  # https://dev.mysql.com/doc/internals/en/compressed-packet-header.html
  when TestWhileIdle:
    conn.lastOperationTime = now()
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
    let send = conn.socket.send(buf,flags = {})
    let success = await withTimeout(send, WriteTimeOut)
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
      let send = conn.socket.send(packet[0].addr,packetLen)
      let success = await withTimeout(send, WriteTimeOut)
      if not success:
        raise newException(TimeoutError, TimeoutErrorMsg)
    else:
      buf[3] = char( conn.sequenceId )
      inc(conn.sequenceId)
      let send = conn.socket.send(buf)
      let success = await withTimeout(send, WriteTimeOut)
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
  when defined(mysql_compression_mode):
    if Cap.zstdCompressionAlgorithm in conn.serverCaps:
      incl(caps, Cap.zstdCompressionAlgorithm)

  conn.clientCaps = caps

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

## MySQL packet packers/unpackers

proc processHeader(c: Connection): nat24 =
  result = c.getPayloadLen
  const errMsg = "Bad packet id (got sequence id $1, expected $2)"
  const errMsg2 = "Bad packet id (got compressed sequence id $1, expected $2)"
  let id = uint8(c.buf[3])
  when defined(mysql_compression_mode):
    if c.use_zstd():
      if id != c.compressedSequenceId:
        raise newException(ProtocolError, errMsg2.format(id,c.compressedSequenceId ) )
      c.compressedSequenceId += 1
    else:
      if id != c.sequenceId:
        raise newException(ProtocolError, errMsg.format(id,c.sequenceId) )
      c.sequenceId += 1
  else:
    if id != c.sequenceId:
      raise newException(ProtocolError, errMsg.format(id,c.sequenceId) )
    c.sequenceId += 1

proc receivePacket*(conn:Connection, drop_ok: bool = false) {.async, tags:[ReadIOEffect,RootEffect].} =
  # drop_ok used when close
  # https://dev.mysql.com/doc/internals/en/uncompressed-payload.html
  conn.bufPos = 0
  conn.buf.setLen(MysqlBufSize)
  zeroMem conn.buf[0].addr,MysqlBufSize
  when TestWhileIdle:
    conn.lastOperationTime = now()
  const TimeoutErrorMsg = "Timeout when receive packet"
  const NormalLen = 4
  const CompressedLen = 7
  var offset:int
  var headerLen:int
  var uncompressedLen:int32
  when not defined(mysql_compression_mode):
    offset = NormalLen
    let rec = conn.socket.recvInto(conn.buf[0].addr, NormalLen,flags = {})
    let success = await withTimeout(rec, ReadTimeOut)
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
      let rec = conn.socket.recvInto(conn.buf[0].addr,CompressedLen)
      let success = await withTimeout(rec, ReadTimeOut)
      if not success:
        raise newException(TimeoutError, TimeoutErrorMsg)
      headerLen = rec.read
      uncompressedLen = int32( uint32(conn.buf[conn.bufPos + 4]) or (uint32(conn.buf[conn.bufPos+5]) shl 8) or (uint32(conn.buf[conn.bufPos+6]) shl 16) )
    else:
      offset = NormalLen
      let rec = conn.socket.recvInto(conn.buf[0].addr,NormalLen)
      let success = await withTimeout(rec, ReadTimeOut)
      if not success:
        raise newException(TimeoutError, TimeoutErrorMsg)
      headerLen = rec.read
  debug "headerLen:" & $headerLen
  if headerLen == 0:
    if drop_ok:
      return 
    else:
      raise newException(ProtocolError, "Connection closed")
  if headerLen != 4 and headerLen != 7:
    raise newException(ProtocolError, "Connection closed unexpectedly")
  conn.payloadLen = conn.processHeader()
  conn.curPayloadLen = conn.payloadLen
  debug "payloadLen:" & $conn.payloadLen
  inc conn.bufPos,offset
  if conn.payloadLen == 0:
    return 
  if offset + conn.payloadLen > MysqlBufSize:
    conn.buf.setLen(offset + conn.payloadLen)
  let payload = conn.socket.recvInto(conn.buf[offset].addr,conn.payloadLen)
  let payloadRecvSuccess = await withTimeout(payload, ReadTimeOut)
  if not payloadRecvSuccess:
    raise newException(TimeoutError, TimeoutErrorMsg)
  conn.bufLen = payload.read
  if conn.bufLen == 0:
    raise newException(ProtocolError, "Connection closed unexpectedly")
  if conn.bufLen != conn.payloadLen:
    raise newException(ProtocolError, "TODO finish this part")
  when defined(mysql_compression_mode):
    if conn.use_zstd():
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
      conn.resetPayloadLen

proc roundtrip*(conn:Connection, data: string) {.async, tags:[IOEffect,RootEffect].} =
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
  inc conn.bufPos,2
  meta[index].length = scanU32(conn.buf, conn.bufPos)
  inc conn.bufPos,4
  meta[index].columnType = FieldType(uint8(conn.buf[conn.bufPos]))
  inc conn.bufPos,1
  meta[index].flags = cast[set[FieldFlag]](scanU16(conn.buf, conn.bufPos))
  inc conn.bufPos,2
  meta[index].decimals = int(conn.buf[conn.bufPos])
  inc conn.bufPos
  inc(conn.bufPos, 2) # filter internals manual mentioned

proc receiveMetadata*(conn: Connection, count: Positive): Future[seq[ColumnDefinition]] {.async.} =
  var received = 0
  result = newSeq[ColumnDefinition](count)
  while received < count:
    await conn.receivePacket()
    if conn.firstByte.uint8 == ResponseCode_ERR or conn.firstByte.uint8 == ResponseCode_EOF:
      raise newException(ProtocolError, "TODO")
    conn.processMetadata(result,received)
    inc(received)
  await conn.receivePacket()
  # If the CLIENT_DEPRECATE_EOF client capability flag is not set, EOF_Packet
  if conn.firstByte.uint8 != ResponseCode_EOF:
    raise newException(ProtocolError, "Expected EOF after column defs, got something else fist byte:0x" & $conn.firstByte.uint8)