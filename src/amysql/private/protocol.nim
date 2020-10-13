
include ./protocol_basic
import ./cap
import asyncnet,asyncdispatch
import ../conn
import times

when defined(mysqlx_compression_mode):
  # he default compression levels are initially set to 3 for zstd, 2 for LZ4, and 3 for Deflate. 
  # https://dev.mysql.com/doc/refman/8.0/en/x-plugin-connection-compression.html#x-plugin-connection-compression-monitoring
  const mysqlx_zstd_default_client_compression_level {.intdefine.}: int = 3
  import zstd

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

proc parseHandshakePacket*(conn: Connection, buf: string): HandshakePacket = 
  new result
  result.protocolVersion = int(buf[0])
  if result.protocolVersion != HandshakeV10.int:
    raise newException(ProtocolError, "Unexpected protocol version: " & $result.protocolVersion)
  var pos = 1
  conn.serverVersion = readNulString(buf, pos)
  result.serverVersion = conn.serverVersion
  conn.threadId = scanU32(buf, pos)
  result.threadId = int(conn.threadId)
  inc(pos,4)
  result.scrambleBuff1 = buf[pos .. pos+7]
  inc(pos,8)
  inc pos # filter0
  let capabilities1 = scanU16(buf, pos)
  result.capabilities1 = int(capabilities1)
  result.capabilities = result.capabilities1
  conn.serverCaps = cast[set[Cap]](capabilities1)
  inc(pos,2)
  result.charset = int(buf[pos])
  inc pos
  result.serverStatus = int(scanU16(buf, pos))
  inc pos,2
  result.protocol41 = (capabilities1 and Cap.protocol41.ord) > 0
  if not result.protocol41:
    raise newException(ProtocolError, "Old (pre-4.1) server protocol")

  let capabilities2 = scanU16(buf, pos)
  result.capabilities2 = int(capabilities2)
  inc pos,2
  let cap = uint32(capabilities1) + (uint32(capabilities2) shl 16)
  conn.serverCaps = cast[set[Cap]]( cap )
  result.capabilities = int(cap)
  result.scrambleLen = int(buf[pos])
  inc pos
  inc pos,10 # filter2
  result.scrambleBuff2 = buf[pos ..< (pos + 12)]
  inc pos,12
  result.scrambleBuff = result.scrambleBuff1 & result.scrambleBuff2
  inc pos # filter 3
  if Cap.pluginAuth in conn.serverCaps:
    result.plugin = readNulStringX(buf, pos)

proc parseAuthSwitchPacket*(conn: Connection, pkt: string): ref ResponseAuthSwitch =
  new(result)
  var pos: int = 1
  result.status = ResponseCode_ExtraAuthData
  result.pluginName = readNulString(pkt, pos)
  result.pluginData = readNulStringX(pkt, pos)

proc parseResponseAuthMorePacket*(conn: Connection,pkt: string): ref ResponseAuthMore =
  new(result)
  var pos: int = 1
  result.status = ResponseCode_ExtraAuthData
  result.pluginData = readNulStringX(pkt, pos)

proc parseOKPacket*(conn: Connection, pkt: string): ResponseOK =
  result.eof = false
  var pos: int = 1
  result.affected_rows = readLenInt(pkt, pos)
  result.last_insert_id = readLenInt(pkt, pos)
  # We always supply Cap.protocol41 in client caps
  result.status_flags = cast[set[Status]]( scanU16(pkt, pos) )
  result.warning_count = scanU16(pkt, pos+2)
  pos = pos + 4
  if Cap.sessionTrack in conn.clientCaps:
    result.info = readLenStr(pkt, pos)
  else:
    result.info = readNulStringX(pkt, pos)

proc parseEOFPacket*(pkt: string): ResponseOK =
  result.eof = true
  result.warning_count = scanU16(pkt, 1)
  result.status_flags = cast[set[Status]]( scanU16(pkt, 3) )

proc sendPacket*(conn: Connection, buf: var string, resetSeqId = false): Future[void] =
  # Caller must have left the first four bytes of the buffer available for
  # us to write the packet header.
  let bodylen = len(buf) - 4
  buf[0] = char( (bodylen and 0xFF) )
  buf[1] = char( ((bodylen shr 8) and 0xFF) )
  buf[2] = char( ((bodylen shr 16) and 0xFF) )
  if resetSeqId:
    conn.sequenceId = 0
  buf[3] = char( conn.sequenceId )
  inc(conn.sequenceId)
  when defined(mysqlx_compression_mode):
    if conn.zstdAvailable and conn.authenticated:
      var compressed = compress(buf,mysqlx_zstd_default_client_compression_level)
      conn.socket.send(compressed.addr, compressed.len)
    else:
      conn.socket.send(buf)
  else:
    conn.socket.send(buf)

proc writeHandshakeResponse*(conn: Connection,
                            username: string,
                            auth_response: string,
                            database: string,
                            auth_plugin: string): Future[void] =
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

proc readTime*(buf: string, pos: var int): Duration = 
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

proc readDateTime*(buf: string, pos: var int, zone: Timezone = utc()): DateTime = 
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

proc processHeader(c: Connection, hdr: array[4, char]): nat24 =
  result = int32(hdr[0]) + int32(hdr[1])*256 + int32(hdr[2])*65536
  let pnum = uint8(hdr[3])
  if pnum != c.sequenceId:
    raise newException(ProtocolError, "Bad packet number (got sequence number " & $(pnum) & ", expected " & $(c.sequenceId) & ")")
  c.sequenceId += 1

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
  when defined(mysqlx_compression_mode):
    if conn.zstdAvailable and conn.authenticated:
      result = cast[string](decompress(result))

proc roundtrip*(conn:Connection, data: string): Future[string] {.async, tags:[IOEffect,RootEffect].} =
  var buf: string = newStringOfCap(32)
  buf.setLen(4)
  buf.add data
  await conn.sendPacket(buf)
  let pkt = await conn.receivePacket()
  if isERRPacket(pkt):
    raise parseErrorPacket(pkt)
  return pkt

proc receiveMetadata*(conn: Connection, count: Positive): Future[seq[ColumnDefinition]] {.async.} =
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