#    MysqlParser - An efficient packet parser for MySQL Client/Server Protocol
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

import ./protocol
import ./cap
include ./status
import ./auth

proc toProtocolHex*(x: Natural, len: Positive): string =
  ## Converts ``x`` to a string in the format of mysql Client/Server Protocol.
  ## For example: `(0xFAFF, 2)` => `"\xFF\xFA"`, `(0xFAFF00, 3)` => `"\x00\xFF\xFA"`. 
  var n = x
  result = newString(len)
  for i in 0..<int(len):
    result[i] = chr(n and 0xFF)
    n = n shr 8

proc toProtocolInt*(str: string): Natural =
  ## Converts ``str`` to a nonnegative integer.
  ## For example: `"\xFF\xFA"` => `0xFAFF`, `"\x00\xFF\xFA"` => `0xFAFF00`.  
  result = 0
  var i = 0
  for c in str:
    inc(result, ord(c) shl (8 * i)) # c.int * pow(16.0, i.float32 * 2).int
    inc(i)

proc offsetChar(x: pointer, i: int): pointer {.inline.} =
  cast[pointer](cast[ByteAddress](x) + i * sizeof(char))

proc offsetCharVal(x: pointer, i: int): char {.inline.} =
  cast[ptr char](offsetChar(x, i))[]

proc joinFixedStr(s: pointer, sLen: int, want: var int, buf: pointer, size: int) =
  # Joins `s` incrementally.
  # It is finished if want is `0` when returned, or not finished.
  var n: int
  if sLen < want:
    if size < sLen:
      n = size
    else:
      n = sLen
  else: 
    if size < want:
      n = size
    else:
      n = want  
  copyMem(s, buf, n)
  dec(want, n)

proc joinFixedStr(s: var string, want: var int, buf: pointer, size: int) =
  # Joins `s` incrementally, `s` is a fixed length string. The first `want` is its length.
  # It is finished if want is `0` when returned, or not finished.
  let n = min(size,want)
  for i in 0..<n:
    add(s, offsetCharVal(buf, i)) 
  dec(want, n)

proc joinNulStr(s: var string, buf: pointer, size: int): tuple[finished: bool, count: int] =
  # Joins `s` incrementally, `s` is a null terminated string. 
  result.finished = false
  for i in 0..<size:
    inc(result.count)
    if offsetCharVal(buf, i) == '\0':
      result.finished = true
      return
    else:
      add(s, offsetCharVal(buf, i))

type
#[ Handshake Initialization Packet
3              packet Length 
1              packet sequenceId
1              [0a] protocolVersion serverVersion
string[NUL]    server serverVersion
4              connection id
string[8]      scramble buff 1
1              [00] filler
2              capability flags (lower 2 bytes)
1              character set
2              serverStatus flags
  if capabilities & Cap.protocol41.ord {
2              capability flags (upper 2 bytes)
1              scramble payloadLen
10             reserved (all [00])
string[12]     scramble buff 2
1              [00] filler
  } else {
13             [00] filler
  }
  if more data in the packet {
string[NUL]    auth-plugin name  
  }
]#
  PacketParserKind* = enum ## Kinds of ``PacketParser``.
    ppkHandshake, ppkCommandResult 
  PacketParser* = ref PacketParserObj
  PacketParserObj = object ## Parser that is used to parse a Mysql Client/Server Protocol packet.
    buf: pointer
    bufLen: int
    bufPos: int
    bufRealLen: int
    word: string
    want: int
    payloadLen: int
    sequenceId: int
    remainingPayloadLen: int
    storedWord: string
    storedWant: int
    storedState: PacketState
    state: PacketState
    wantEncodedState: LenEncodedState
    case kind: PacketParserKind
    of ppkHandshake:
      discard
    of ppkCommandResult:
      command: Command 
    isEntire: bool

  LenEncodedState = enum # Parse state for length encoded integer or string.
    lenFlagVal, lenIntVal, lenStrVal

  ProgressState = enum # Progress state for parsing.
    prgOk, prgNext, prgEmpty

  PacketState* = enum # Parsing state of the ``PacketParser``.
    packInit, 
    packHeader, 
    packFinish, 
    packHandshake, 
    packResultHeader,
    packResultOk, 
    packResultError,
    packResultSetFields, 
    packResultSetRows

  EofState = enum
    eofHeader, 
    eofWarningCount, 
    eofServerStatus

  EofPacket* = object
    warningCount*: int
    serverStatus*: int 
    state: EofState

  ResultPacketKind* = enum ## Kinds of result packet.
    rpkOk, rpkError, rpkResultSet  

  OkState = enum
    okAffectedRows, 
    okLastInsertId, 
    okServerStatus, 
    okWarningCount, 
    okMessage

  ErrorState = enum
    errErrorCode, 
    errSqlState, 
    errSqlStateMarker, 
    errErrorMessage

  FieldState = enum
    fieldCatalog,    
    fieldSchema,      
    fieldTable,       
    fieldOrgTable,   
    fieldName,        
    fieldOrgName,
    fieldFiller1,    
    fieldCharset,     
    fieldLen,   
    fieldType,       
    fieldFlags,       
    fieldDecimals,    
    fieldFiller2,    
    fieldDefaultValue

  FieldPacket* = object 
    catalog*: string
    schema*: string
    table*: string
    orgTable*: string
    name*: string
    orgName*: string
    charset*: int
    fieldLen*: int
    fieldType*: int
    fieldFlags*: int
    decimals*: int
    defaultValue*: string
    state: FieldState

  ResultSetState = enum
    rsetExtra, 
    rsetFieldHeader, 
    rsetField, 
    rsetFieldEof, 
    rsetRowHeader, 
    rsetRowHeaderLen,
    rsetRow, 
    rsetRowEof

  RowsState* = enum
    rowsFieldBegin,
    rowsFieldFull,
    rowsFieldEnd,
    rowsBufEmpty,
    rowsFinished

  ResultPacket* = object ## The result packet object.
    sequenceId*: int           
    case kind*: ResultPacketKind
    of rpkOk:
      affectedRows*: int
      lastInsertId*: int
      serverStatus*: int
      warningCount*: int
      message*: string
      okState: OkState
    of rpkError:
      errorCode*: int  
      sqlStateMarker*: string
      sqlState*: string
      errorMessage*: string
      errState: ErrorState
    of rpkResultSet:
      extra*: string
      fieldsCount*: int        
      fieldsPos: int
      fields*: seq[FieldPacket]
      fieldsEof: EofPacket
      rowsEof: EofPacket
      rsetState: ResultSetState
      fieldLen: int
      fieldBuf: pointer
      fieldBufLen: int
      fieldMeetNull: bool
      hasRows*: bool
    hasMoreResults: bool

  RowList* = object
    value*: seq[string]
    counter: int


proc initHandshakePacket(pkt:HandshakePacket) =
 
  pkt.protocol41 = true
  pkt.state = hssProtocolVersion

proc newHandshakePacket(): HandshakePacket =
  new result
  initHandshakePacket(result)

proc initEofPacket(): EofPacket =
  result.warningCount = 0
  result.serverStatus = 0
  result.state = eofHeader   

proc initFieldPacket(): FieldPacket =

  result.state = fieldCatalog

proc initResultPacket(kind: ResultPacketKind): ResultPacket =
  result.kind = kind
  case kind
  of rpkOk:
    result.affectedRows = 0
    result.lastInsertId = 0
    result.serverStatus = 0
    result.warningCount = 0
    result.message = ""
    result.okState = okAffectedRows
  of rpkError:
    result.errorCode = 0
    result.sqlStateMarker = ""
    result.sqlState = ""
    result.errorMessage = ""
    result.errState = errErrorCode
  of rpkResultSet:
    result.extra = ""
    result.fieldsPos = 0
    result.fields = @[]
    result.fieldsEof = initEofPacket()
    result.rowsEof = initEofPacket()
    result.rsetState = rsetExtra
    result.hasRows = false
    result.fieldMeetNull = false
  result.hasMoreResults = false

proc initRowList*(): RowList =
  result.value = @[]
  result.counter = -1

proc newPacketParser( state = packInit ):PacketParser = 
  new result

  case state 
  of packInit:
    result.want = 4 
  of packHandshake:
    result.want = 1
  else:
    discard
  
  result.storedState = state
  result.state = state
  result.wantEncodedState = lenFlagVal
  result.isEntire = true 

proc newPacketParser*(kind: PacketParserKind, state = packInit): PacketParser = 
  ## Creates a new packet parser for parsing a handshake connection.
  result = newPacketParser(state)
  result.kind = kind

proc newPacketParser*(command: Command, state = packInit): PacketParser = 
  ## Creates a new packet parser for receiving a result packet.
  result = newPacketParser(state)
  result.kind = ppkCommandResult
  result.command = command
  
proc finished*(p: PacketParser): bool =
  ## Determines whether ``p`` has completed.
  result = p.state == packFinish

proc sequenceId*(parser: PacketParser): int = 
  ## Gets the current sequence ID.
  result = parser.sequenceId

proc offset*(parser: PacketParser): int =
  ## Gets the offset of the latest buffer. 
  result = parser.bufPos

proc buffered*(p: PacketParser): bool =
  result = p.bufPos < p.bufLen

proc loadBuffer*(p: PacketParser, buf: pointer, size: int) = 
  p.buf = buf
  p.bufLen = size
  p.bufPos = 0
  if p.state != packInit and p.state != packHeader:
    p.bufRealLen = if p.remainingPayloadLen <= size: p.remainingPayloadLen
                   else: size

proc loadBuffer*(p: PacketParser, buf: string) =
  loadBuffer(p, buf.cstring, buf.len)

proc move(p: PacketParser) = 
  assert p.bufRealLen == 0
  assert p.remainingPayloadLen == 0
  p.storedState = p.state
  p.storedWant = p.want
  p.storedWord = p.word
  p.state = packHeader
  p.want = 4  
  p.word = ""
  p.isEntire = true 

proc parseHeader(p: PacketParser): ProgressState =
  result = prgOk
  let w = p.want
  joinFixedStr(p.word, p.want, offsetChar(p.buf, p.bufPos), p.bufLen - p.bufPos)
  inc(p.bufPos, w - p.want)
  if p.want > 0: 
    return prgEmpty
  p.payloadLen = toProtocolInt(p.word[0..2])
  p.sequenceId = toProtocolInt(p.word[3..3])
  p.remainingPayloadLen = p.payloadLen
  p.bufRealLen = if p.bufLen - p.bufPos > p.remainingPayloadLen: p.remainingPayloadLen
                 else: p.bufLen - p.bufPos
  if p.payloadLen == 0xFFFFFF:
    p.isEntire = false
  elif p.payloadLen == 0:
    p.isEntire = true
  p.state = p.storedState
  p.want = p.storedWant
  p.word = p.storedWord
  p.storedState = packInit

proc checkIfMove(p: PacketParser): ProgressState =
  assert p.bufRealLen == 0
  if p.bufLen > p.bufPos:
    assert p.remainingPayloadLen == 0
    move(p)
    return prgNext
  else: 
    if p.remainingPayloadLen > 0:
      return prgEmpty
    else:
      move(p)
      return prgEmpty

proc parseFixed(p: PacketParser, field: var int): ProgressState =
  result = prgOk
  if p.want == 0:
    field = 0
    return
  if p.bufRealLen == 0:
    return checkIfMove(p)
  let want = p.want
  joinFixedStr(p.word, p.want, offsetChar(p.buf, p.bufPos), p.bufRealLen)
  let n = want - p.want
  inc(p.bufPos, n)
  dec(p.bufRealLen, n)
  dec(p.remainingPayloadLen, n)
  if p.want > 0:
    return checkIfMove(p)
  field = toProtocolInt(p.word)
  setLen(p.word, 0)

proc parseFixed(p: PacketParser, buf: pointer, size: int): (ProgressState, bool) =
  if p.want == 0:
    return (prgOk, false)
  if p.bufRealLen == 0:
    return (checkIfMove(p), false)
  let want = p.want
  joinFixedStr(buf, size, p.want, offsetChar(p.buf, p.bufPos), p.bufRealLen)
  let n = want - p.want
  inc(p.bufPos, n)
  dec(p.bufRealLen, n)
  dec(p.remainingPayloadLen, n)
  if p.want > 0:
    if p.bufRealLen == 0:
      return (checkIfMove(p), false)
    else:
      return (prgOk, true)

proc parseFixed(p: PacketParser, field: var string): ProgressState =
  result = prgOk
  if p.want == 0:
    return
  if p.bufRealLen == 0:
    return checkIfMove(p)
  let want = p.want
  joinFixedStr(field, p.want, offsetChar(p.buf, p.bufPos), p.bufRealLen)
  let n = want - p.want
  inc(p.bufPos, n)
  dec(p.bufRealLen, n)
  dec(p.remainingPayloadLen, n)
  if p.want > 0:
    return checkIfMove(p)

proc parseNul(p: PacketParser, field: var string): ProgressState =
  result = prgOk
  if p.bufRealLen == 0:
    return checkIfMove(p)
  let (finished, count) = joinNulStr(field, offsetChar(p.buf, p.bufPos), p.bufRealLen)
  inc(p.bufPos, count)
  dec(p.bufRealLen, count)
  dec(p.remainingPayloadLen, count)
  if not finished:
    return checkIfMove(p)

proc parseFiller(p: PacketParser): ProgressState =
  result = prgOk
  if p.want > p.bufRealLen:
    inc(p.bufPos, p.bufRealLen)
    dec(p.remainingPayloadLen, p.bufRealLen)
    dec(p.want, p.bufRealLen)
    dec(p.bufRealLen, p.bufRealLen)
    return checkIfMove(p)
  else:  
    let n = p.want
    inc(p.bufPos, n)
    dec(p.bufRealLen, n)
    dec(p.remainingPayloadLen, n)
    dec(p.want, n)

proc parseLenEncoded(p: PacketParser, field: var int): ProgressState =
  while true:
    case p.wantEncodedState
    of lenFlagVal:
      var value: int
      let ret = parseFixed(p, value)
      if ret != prgOk:
        return ret
      assert value >= 0
      if value < 251:
        field = value
        return prgOk
      elif value == 0xFC:
        p.want = 2
      elif value == 0xFD:
        p.want = 3
      elif value == 0xFE:
        p.want = 8
      else:
        raise newException(ValueError, "bad encoded flag " & toProtocolHex(value, 1))  
      p.wantEncodedState = lenIntVal
    of lenIntVal:
      return parseFixed(p, field)
    else:
      raise newException(ValueError, "unexpected state " & $p.wantEncodedState)

proc parseLenEncoded(p: PacketParser, field: var string): ProgressState =
  while true:
    case p.wantEncodedState
    of lenFlagVal:
      var value: int
      let ret = parseFixed(p, value)
      if ret != prgOk:
        return ret
      assert value >= 0
      if value < 251:
        p.wantEncodedState = lenStrVal
        p.want = value
        continue
      elif value == 0xFB: # 0xFB means that this string field is ``NULL``
        field = ""
        return prgOk
      elif value == 0xFC:
        p.want = 2
      elif value == 0xFD:
        p.want = 3
      elif value == 0xFE:
        p.want = 8
      else:
        raise newException(ValueError, "bad encoded flag " & toProtocolHex(value, 1))  
      p.wantEncodedState = lenIntVal
    of lenIntVal:
      var value: int
      let ret = parseFixed(p, value)
      if ret != prgOk:
        return ret
      p.want = value
      p.wantEncodedState = lenStrVal
    of lenStrVal:
      return parseFixed(p, field)

template checkPrg(state: ProgressState): untyped =
  case state
  of prgOk:
    discard
  of prgNext:
    continue
  of prgEmpty:
    return false

template checkIfOk(state: ProgressState): untyped =
  case state
  of prgOk:
    discard
  of prgNext:
    return prgNext
  of prgEmpty:
    return prgEmpty

proc parseHandshakeProgress(p: PacketParser, packet: HandshakePacket): ProgressState = 
  while true:
    case packet.state
    of hssProtocolVersion:
      checkIfOk parseFixed(p, packet.protocolVersion)
      packet.state = hssServerVersion
    of hssServerVersion:
      checkIfOk parseNul(p, packet.serverVersion)
      packet.state = hssThreadId
      p.want = 4
    of hssThreadId: # connection id
      checkIfOk parseFixed(p, packet.threadId)
      packet.state = hssScrambleBuff1
      p.want = 8
    of hssScrambleBuff1:
      checkIfOk parseFixed(p, packet.scrambleBuff1)
      packet.state = hssFiller0
      p.want = 1
    of hssFiller0:
      checkIfOk parseFiller(p)
      packet.state = hssCapabilities1
      p.want = 2
    of hssCapabilities1:
      checkIfOk parseFixed(p, packet.capabilities1)
      packet.state = hssCharSet
      p.want = 1
    of hssCharSet:
      checkIfOk parseFixed(p, packet.charset)
      packet.state = hssStatus
      p.want = 2
    of hssStatus:
      checkIfOk parseFixed(p, packet.serverStatus)
      packet.protocol41 = (packet.capabilities1 and Cap.protocol41.ord ) > 0
      if packet.protocol41:
        packet.state = hssCapabilities2
        p.want = 2
      else:
        packet.state = hssFiller3
        p.want = 13
    of hssCapabilities2:
      checkIfOk parseFixed(p, packet.capabilities2)
      packet.capabilities = packet.capabilities1 + 65536 * packet.capabilities2 # 16*16*16*1
      packet.state = hssFiller1
      p.want = 1
    of hssFiller1:
      checkIfOk parseFixed(p, packet.scrambleLen)
      packet.state = hssFiller2
      p.want = 10
    of hssFiller2:
      checkIfOk parseFiller(p)
      # scrambleBuff2 should be 0x00 terminated, but sphinx does not do this
      # so we assume scrambleBuff2 to be 12 byte and treat the next byte as a
      # filler byte.
      packet.state = hssScrambleBuff2
      p.want = 12
    of hssScrambleBuff2:
      checkIfOk parseFixed(p, packet.scrambleBuff2)
      packet.scrambleBuff = packet.scrambleBuff1 & packet.scrambleBuff2
      packet.state = hssFiller3
      p.want = 1
    of hssFiller3:
      checkIfOk parseFiller(p)
      # if p.isEntire and p.remainingPayloadLen == 0:
      if p.remainingPayloadLen == 0:
        packet.sequenceId = p.sequenceId
        return prgOk
      else:  
        packet.state = hssPlugin
    of hssPlugin:
      # According to the docs this should be 0x00 terminated, but MariaDB does
      # not do this, so we assume this string to be packet terminated.
      checkIfOk parseNul(p, packet.plugin)
      packet.sequenceId = p.sequenceId
      return prgOk

proc parseHandshake*(p: PacketParser, packet: HandshakePacket): bool = 
  ## Parses the buffer data in ``buf``. ``size`` is the length of ``buf``.
  ## If parsing is complete, ``p``.``finished`` will be ``true``.
  while true:
    case p.state
    of packInit:
      initHandshakePacket(packet)
      p.state = packHandshake
      p.want = 1
      move(p)
    of packHeader:
      checkPrg parseHeader(p)
    of packHandshake:
      checkPrg parseHandshakeProgress(p, packet)
      p.state = packFinish
    of packFinish:
      return true
    else:
      raise newException(ValueError, "unexpected state " & $p.state)

proc parseResultHeader*(p: PacketParser, packet: var ResultPacket): bool = 
  ## Parses the buffer data in ``buf``. ``size`` is the length of ``buf``.
  ## If parsing is complete, ``p``.``finished`` will be ``true``.
  while true:
    case p.state
    of packInit:
      p.state = packResultHeader
      p.want = 1
      move(p)
    of packHeader:
      checkPrg parseHeader(p)
    of packResultHeader:
      var header: int
      checkPrg parseFixed(p, header)
      case header
      of 0x00:
        packet = initResultPacket(rpkOk)
        p.state = packResultOk
        p.want = 1
        p.wantEncodedState = lenFlagVal
      of 0xFF:
        packet = initResultPacket(rpkError)
        p.state = packResultError
        p.want = 2
        p.wantEncodedState = lenFlagVal
      else:
        packet = initResultPacket(rpkResultSet)
        packet.fieldsCount = header
        p.state = packResultSetFields
        p.want = p.remainingPayloadLen
    of packResultOk, packResultError, packResultSetFields:
      return true
    else:
      raise newException(ValueError, "unexpected state " & $p.state)

proc parseOkProgress(p: PacketParser, packet: var ResultPacket, capabilities: int): ProgressState =
  template checkHowStatusInfo: untyped =
    # if (capabilities and CLIENT_SESSION_TRACK) > 0 and p.remainingPayloadLen > 0:
    #   packet.okState = okStatusInfo
    #   p.want = 1
    #   p.wantEncodedState = lenFlagVal
    # else:
    #   packet.okState = okStatusInfo
    #   p.want = p.remainingPayloadLen
    packet.okState = okMessage
    p.want = p.remainingPayloadLen
  while true:
    case packet.okState
    of okAffectedRows:
      checkIfOk parseLenEncoded(p, packet.affectedRows)
      packet.okState = okLastInsertId
      p.want = 1
      p.wantEncodedState = lenFlagVal
    of okLastInsertId:
      checkIfOk parseLenEncoded(p, packet.lastInsertId)
      if (capabilities and Cap.protocol41.ord) > 0 or 
         (capabilities and Cap.transactions.ord) > 0:
        packet.okState = okServerStatus
        p.want = 2
      else:
        checkHowStatusInfo
    of okServerStatus:
      checkIfOk parseFixed(p, packet.serverStatus)
      packet.hasMoreResults = (packet.serverStatus and SERVER_MORE_RESULTS_EXISTS) > 0
      if (capabilities and Cap.protocol41.ord) > 0:
        packet.okState = okWarningCount
        p.want = 2
      else:
        checkHowStatusInfo
    of okWarningCount:
      checkIfOk parseFixed(p, packet.warningCount)
      checkHowStatusInfo
    of okMessage:
      checkIfOk parseFixed(p, packet.message)
      packet.sequenceId = p.sequenceId
      return prgOk
    # of okStatusInfo:
    #   if (capabilities and CLIENT_SESSION_TRACK) > 0 and p.remainingPayloadLen > 0:
    #     checkIfOk parseLenEncoded(p, packet.message)
    #     packet.okState = okSessionState
    #     p.want = 1
    #     p.wantEncodedState = lenFlagVal
    #   else:
    #     checkIfOk parseFixed(p, packet.message)
    #     packet.sequenceId = p.sequenceId
    #     return prgOk
    # of okSessionState:
    #   checkIfOk parseLenEncoded(p, packet.sessionState)
    #   packet.sequenceId = p.sequenceId
    #   return prgOk

proc parseOk*(p: PacketParser, packet: var ResultPacket, capabilities: int): bool =
  while true:
    case p.state
    of packHeader:
      checkPrg parseHeader(p)
    of packResultOk:
      checkPrg parseOkProgress(p, packet, capabilities)
      p.state = packFinish
    of packFinish:
      return true
    else:
      raise newException(ValueError, "unexpected state " & $p.state)  

proc parseErrorProgress(p: PacketParser, packet: var ResultPacket, capabilities: int): ProgressState =
  while true:
    case packet.errState
    of errErrorCode:
      checkIfOk parseFixed(p, packet.errorCode)
      if (capabilities and Cap.protocol41.ord) > 0:
        packet.errState = errSqlStateMarker
        p.want = 1
      else:
        packet.errState = errErrorMessage
        p.want = p.remainingPayloadLen
    of errSqlStateMarker:
      checkIfOk parseFixed(p, packet.sqlStateMarker)
      packet.errState = errSqlState
      p.want = 5
    of errSqlState:
      checkIfOk parseFixed(p, packet.sqlState)
      packet.errState = errErrorMessage
      p.want = p.remainingPayloadLen
    of errErrorMessage:
      checkIfOk parseFixed(p, packet.errorMessage)
      packet.sequenceId = p.sequenceId
      return prgOk

proc parseError*(p: PacketParser, packet: var ResultPacket, capabilities: int): bool =
  while true:
    case p.state
    of packHeader:
      checkPrg parseHeader(p)
    of packResultError:
      checkPrg parseErrorProgress(p, packet, capabilities)
      p.state = packFinish
    of packFinish:
      return true
    else:
      raise newException(ValueError, "unexpected state " & $p.state)  

proc parseEofProgress(p: PacketParser, packet: var EofPacket, capabilities: int): ProgressState =
  while true:
    case packet.state
    of eofHeader:
      if (capabilities and Cap.protocol41.ord) > 0:
        packet.state = eofWarningCount
        p.want = 2
      else:
        assert p.remainingPayloadLen == 0
        return prgOk
    of eofWarningCount:
      checkIfOk parseFixed(p, packet.warningCount)
      packet.state = eofServerStatus
      p.want = 2
    of eofServerStatus:
      checkIfOk parseFixed(p, packet.serverStatus)
      assert p.remainingPayloadLen == 0
      return prgOk

proc parseFieldProgress(p: PacketParser, packet: var FieldPacket, capabilities: int): ProgressState =
  while true:
    case packet.state
    of fieldCatalog:
      if (capabilities and Cap.protocol41.ord) > 0:
        checkIfOk parseFixed(p, packet.catalog)
        packet.state = fieldSchema
        p.want = 1
        p.wantEncodedState = lenFlagVal
      else:
        packet.state = fieldTable
        # p.want = 1
        # p.wantEncodedState = lenFlagVal
    of fieldSchema:
      checkIfOk parseLenEncoded(p, packet.schema)
      packet.state = fieldTable
      p.want = 1
      p.wantEncodedState = lenFlagVal
    of fieldTable:
      #checkIfOk parseLenEncoded(p, packet.table)
      if (capabilities and Cap.protocol41.ord) > 0:
        checkIfOk parseLenEncoded(p, packet.table)
        packet.state = fieldOrgTable
        p.want = 1
        p.wantEncodedState = lenFlagVal
      else:
        checkIfOk parseFixed(p, packet.table)
        packet.state = fieldName
        p.want = 1
        p.wantEncodedState = lenFlagVal
    of fieldOrgTable:
      checkIfOk parseLenEncoded(p, packet.orgTable)
      packet.state = fieldName
      p.want = 1
      p.wantEncodedState = lenFlagVal
    of fieldName:
      checkIfOk parseLenEncoded(p, packet.name)
      if (capabilities and Cap.protocol41.ord) > 0:
        packet.state = fieldOrgName
        p.want = 1
        p.wantEncodedState = lenFlagVal
      else:
        packet.state = fieldLen
        p.want = 4
    of fieldOrgName:
      checkIfOk parseLenEncoded(p, packet.orgName)
      packet.state = fieldFiller1
      p.want = 1
      p.wantEncodedState = lenFlagVal
    of fieldFiller1:
      var fieldsLen: int
      checkIfOk parseLenEncoded(p, fieldsLen)
      assert fieldsLen == 0x0c
      packet.state = fieldCharset
      p.want = 2
    of fieldCharset:
      checkIfOk parseFixed(p, packet.charset)
      packet.state = fieldLen
      p.want = 4
    of fieldLen:
      checkIfOk parseFixed(p, packet.fieldLen)  
      packet.state = fieldType
      if (capabilities and Cap.protocol41.ord) > 0:
        p.want = 1
      else:
        p.want = 2
    of fieldType:
      checkIfOk parseFixed(p, packet.fieldType)
      packet.state = fieldFlags
      p.want = 2
    of fieldFlags:
      checkIfOk parseFixed(p, packet.fieldFlags)
      packet.state = fieldDecimals
      p.want = 1
    of fieldDecimals:
      checkIfOk parseFixed(p, packet.decimals)
      if (capabilities and Cap.protocol41.ord) > 0:
        packet.state = fieldFiller2
        p.want = 2
      else:
        packet.state = fieldDefaultValue
        p.want = p.remainingPayloadLen
    of fieldFiller2:
      checkIfOk parseFiller(p)
      if p.command == Command.fieldList: 
        packet.state = fieldDefaultValue
        p.want = 1
        p.wantEncodedState = lenFlagVal
      else:
        assert p.remainingPayloadLen == 0
        return prgOk
    of fieldDefaultValue:
      checkIfOk parseLenEncoded(p, packet.defaultValue)
      assert p.remainingPayloadLen == 0
      return prgOk

proc parseFields*(p: PacketParser, packet: var ResultPacket, capabilities: int): bool =
  template checkIfOk(state: ProgressState): untyped =
    case state
    of prgOk:
      discard
    of prgNext:
      break
    of prgEmpty:
      return false
  while true:
    case p.state
    of packHeader:
      checkPrg parseHeader(p)
    of packResultSetFields:
      while true:
        case packet.rsetState
        of rsetExtra:
          if p.want > 0:
            checkIfOk parseFixed(p, packet.extra)
          p.want = 1
          packet.rsetState = rsetFieldHeader
        of rsetFieldHeader: 
          var header: int
          checkIfOk parseFixed(p, header)
          if header == 0xFE and p.payloadLen < 9:
            packet.rsetState = rsetFieldEof
            p.want = 1
          else:
            var field = initFieldPacket()
            add(packet.fields, field)
            packet.rsetState = rsetField
            p.want = header
        of rsetField:
          checkIfOk parseFieldProgress(p, packet.fields[packet.fieldsPos], capabilities)
          packet.rsetState = rsetFieldHeader
          p.want = 1
          inc(packet.fieldsPos)
        of rsetFieldEof:
          checkIfOk parseEofProgress(p, packet.fieldsEof, capabilities) 
          if p.command == Command.fieldList:
            packet.hasMoreResults = (packet.fieldsEof.serverStatus and SERVER_MORE_RESULTS_EXISTS) > 0
            packet.sequenceId = p.sequenceId
            packet.hasRows = false
            p.state = packFinish
            break
          else:
            packet.rsetState = rsetRowHeader
            packet.hasRows = true
            p.state = packResultSetRows
            p.want = 1
            p.wantEncodedState = lenFlagVal 
            break
        else:
          raise newException(ValueError, "unexpected state " & $packet.rsetState) 
    of packResultSetRows:
      return true
    of packFinish:
      return true
    else:
      raise newException(ValueError, "unexpected state " & $p.state) 

proc hasMoreResults*(packet: ResultPacket): bool =
  result = packet.hasMoreResults

proc allocPasingField*(packet: var ResultPacket, buf: pointer, size: int) =
  packet.fieldBuf = buf
  packet.fieldBufLen = size

proc lenPasingField*(packet: ResultPacket): int =
  result = packet.fieldLen

proc parseRows*(p: PacketParser, packet: var ResultPacket, capabilities: int): 
               tuple[offset: int, state: RowsState] =
  template checkIfOk(state: ProgressState): untyped =
    case state
    of prgOk:
      discard
    of prgNext:
      break
    of prgEmpty:
      return (0, rowsBufEmpty)
  while true:
    case p.state
    of packHeader:
      let prgState = parseHeader(p)
      case prgState
      of prgOk:
        discard
      of prgNext:
        continue
      of prgEmpty:
        return (0, rowsBufEmpty)
    of packResultSetRows:
      while true:
        case packet.rsetState
        of rsetRowHeader: 
          var header: int
          checkIfOk parseFixed(p, header)
          if header == 0xFE and p.payloadLen < 9:
            packet.rsetState = rsetRowEof
            p.want = 1
          elif header == 0xFB:
            packet.rsetState = rsetRow
            packet.fieldMeetNull = true
            packet.fieldLen = 1
            return (0, rowsFieldBegin)
          else:
            # packet.rsetState = rsetRow
            # p.want = header
            # assert p.want > 0
            # packet.fieldLen = header
            # return (0, rowsFieldBegin)
            if header < 251:
              p.want = header
              assert p.want > 0
              packet.rsetState = rsetRow
              packet.fieldLen = header
              return (0, rowsFieldBegin)
            elif header == 0xFC:
              p.want = 2
              packet.rsetState = rsetRowHeaderLen
            elif header == 0xFD:
              p.want = 3
              packet.rsetState = rsetRowHeaderLen
            elif header == 0xFE:
              p.want = 8
              packet.rsetState = rsetRowHeaderLen
            else:
              raise newException(ValueError, "bad encoded flag " & toProtocolHex(header, 1))  
        of rsetRowHeaderLen: 
          var header: int
          checkIfOk parseFixed(p, header)
          p.want = header
          assert p.want > 0
          packet.rsetState = rsetRow
          packet.fieldLen = header
          return (0, rowsFieldBegin)
        of rsetRow:
          assert packet.fieldBufLen > 0
          packet.fieldLen = 0
          if packet.fieldMeetNull:
            packet.fieldMeetNull = false
            cast[ptr char](packet.fieldBuf)[] = '\0' # NULL ==> '\0' 
            packet.rsetState = rsetRowHeader
            p.want = 1
            p.wantEncodedState = lenFlagVal 
            return (1, rowsFieldEnd)
          else:  
            let w = p.want
            let (prgState, full) = parseFixed(p, packet.fieldBuf, packet.fieldBufLen)
            let offset = w - p.want
            if full:
              return (offset, rowsFieldFull)
            case prgState
            of prgOk:
              packet.rsetState = rsetRowHeader
              p.want = 1
              p.wantEncodedState = lenFlagVal 
              return (offset, rowsFieldEnd)
            of prgNext:
              break
            of prgEmpty:
              return (offset, rowsBufEmpty)
        of rsetRowEof:
          checkIfOk parseEofProgress(p, packet.rowsEof, capabilities)
          packet.hasMoreResults = (packet.rowsEof.serverStatus and SERVER_MORE_RESULTS_EXISTS) > 0
          packet.sequenceId = p.sequenceId
          p.state = packFinish
          break
        else:
          raise newException(ValueError, "unexpected state " & $packet.rsetState) 
    of packFinish:
      return (0, rowsFinished)
    else:
      raise newException(ValueError, "unexpected state " & $p.state)

proc parseRows*(p: PacketParser, packet: var ResultPacket, capabilities: int, 
                rows: var RowList): bool =
  template checkIfOk(state: ProgressState): untyped =
    case state
    of prgOk:
      discard
    of prgNext:
      break
    of prgEmpty:
      return false
  while true:
    case p.state
    of packHeader:
      checkPrg parseHeader(p)
    of packResultSetRows:
      while true:
        case packet.rsetState
        of rsetRowHeader: 
          var header: int
          checkIfOk parseFixed(p, header)
          if header == 0xFE and p.payloadLen < 9:
            packet.rsetState = rsetRowEof
            p.want = 1
          elif header == 0xFB:
            packet.rsetState = rsetRow
            packet.fieldMeetNull = true
            inc(rows.counter)
            add(rows.value, newStringOfCap(1))
          else:
            # packet.rsetState = rsetRow
            # p.want = header
            # inc(rows.counter)
            # add(rows.value, newStringOfCap(header))
            if header < 251:
              p.want = header
              packet.rsetState = rsetRow
              inc(rows.counter)
              add(rows.value, newStringOfCap(header))
            elif header == 0xFC:
              p.want = 2
              packet.rsetState = rsetRowHeaderLen
            elif header == 0xFD:
              p.want = 3
              packet.rsetState = rsetRowHeaderLen
            elif header == 0xFE:
              p.want = 8
              packet.rsetState = rsetRowHeaderLen
            else:
              raise newException(ValueError, "bad encoded flag " & toProtocolHex(header, 1))  
        of rsetRowHeaderLen: 
          var header: int
          checkIfOk parseFixed(p, header)
          p.want = header
          packet.rsetState = rsetRow
          inc(rows.counter)
          add(rows.value, newStringOfCap(header))
        of rsetRow:
          if packet.fieldMeetNull:
            packet.fieldMeetNull = false
            rows.value[rows.counter] = "" # NULL ==> nil
            packet.rsetState = rsetRowHeader
            p.want = 1
            p.wantEncodedState = lenFlagVal 
          else: 
            checkIfOk parseFixed(p, rows.value[rows.counter])
            packet.rsetState = rsetRowHeader
            p.want = 1
            p.wantEncodedState = lenFlagVal 
        of rsetRowEof:
          checkIfOk parseEofProgress(p, packet.rowsEof, capabilities)
          packet.hasMoreResults = (packet.rowsEof.serverStatus and SERVER_MORE_RESULTS_EXISTS) > 0
          packet.sequenceId = p.sequenceId
          p.state = packFinish
          break
        else:
          raise newException(ValueError, "unexpected state " & $packet.rsetState) 
    of packFinish:
      return true
    else:
      raise newException(ValueError, "unexpected state " & $p.state)

type
  ClientAuthenticationPacket* = object ## The authentication packet for the handshaking connection.
    ## Packet for login request.
    sequenceId*: int           # 1
    capabilities*: int         # 4
    maxPacketSize*: int        # 4
    charset*: int              # [1]
    # filler: string           # [23]
    user*: string              # NullTerminatedString
    # scrambleLen              # 1
    scrambleBuff*: string      # 20
    database*: string          # NullTerminatedString
    protocol41*: bool

  ChangeUserPacket* = object ## The packet for the change user command.
    ## Packet for change user.
    sequenceId*: int           # 1
    user*: string              # NullTerminatedString
    # scrambleLen              # 1
    scrambleBuff*: string      # 
    database*: string          # NullTerminatedString
    charset*: int              # [1]


proc formatClientAuth*(packet: ClientAuthenticationPacket, password: string): string = 
  ## Converts ``packet`` to a string.
  if packet.protocol41:
    let payloadLen = 4 + 4 + 1 + 23 + (packet.user.len + 1) + (1 + 20) + 
                     (packet.database.len + 1)
    result = newStringOfCap(4 + payloadLen)
    add(result, toProtocolHex(payloadLen, 3))
    add(result, toProtocolHex(packet.sequenceId, 1))
    add(result, toProtocolHex(packet.capabilities, 4))
    add(result, toProtocolHex(packet.maxPacketSize, 4))
    add(result, toProtocolHex(packet.charset, 1))
    add(result, toProtocolHex(0, 23))
    add(result, packet.user)
    add(result, '\0')
    add(result, toProtocolHex(20, 1))
    add(result, scramble_native_password(packet.scrambleBuff, password))
    add(result, packet.database)
    add(result, '\0')
  else:
    let payloadLen = 2 + 3 + (packet.user.len + 1) + 
                     8 + 1 + (packet.database.len + 1)
    result = newStringOfCap(4 + payloadLen)                
    add(result, toProtocolHex(payloadLen, 3))
    add(result, toProtocolHex(packet.sequenceId, 1))
    add(result, toProtocolHex(packet.capabilities, 2))
    add(result, toProtocolHex(packet.maxPacketSize, 3))
    add(result, packet.user)
    add(result, '\0')
    add(result, scramble323(packet.scrambleBuff[0..7], password))
    add(result, toProtocolHex(0, 1))
    add(result, packet.database)
    add(result, '\0')

template formatNoArgsComImpl(cmd: Command) = 
  const payloadLen = 1
  result = newStringOfCap(4 + payloadLen)
  add(result, toProtocolHex(payloadLen, 3))
  add(result, toProtocolHex(0, 1))
  add(result, toProtocolHex(cmd.int, 1))

template formatRestStrComImpl(cmd: Command, str: string) = 
  let payloadLen = str.len + 1
  result = newStringOfCap(4 + payloadLen)
  add(result, toProtocolHex(payloadLen, 3))
  add(result, toProtocolHex(0, 1))
  add(result, toProtocolHex(cmd.int, 1))
  add(result, str)

proc formatComQuit*(): string = 
  ## Converts to a ``COM_QUIT`` (mysql Client/Server Protocol) string.
  formatNoArgsComImpl Command.quit

proc formatComInitDb*(database: string): string = 
  ## Converts to a ``COM_INIT_DB`` (mysql Client/Server Protocol) string.
  formatRestStrComImpl Command.initDb, database

proc formatComQuery*(sql: string): string = 
  ## Converts to a ``COM_QUERY`` (mysql Client/Server Protocol) string.
  formatRestStrComImpl Command.query, sql

proc formatComChangeUser*(packet: ChangeUserPacket, password: string): string = 
  ## Converts to a ``COM_CHANGE_USER`` (mysql Client/Server Protocol) string.
  let payloadLen = 1 + (packet.user.len + 1) + (1 + 20) + (packet.database.len + 1) + 2 
  result = newStringOfCap(4 + payloadLen)
  add(result, toProtocolHex(payloadLen, 3))
  add(result, toProtocolHex(0, 1))
  add(result, toProtocolHex(Command.changeUser.int, 1))
  add(result, packet.user)
  add(result, '\0')
  add(result, toProtocolHex(20, 1))
  add(result, scramble_native_password(packet.scrambleBuff, password))
  add(result, packet.database)
  add(result, '\0')
  add(result, toProtocolHex(packet.charset, 2))

proc formatComPing*(): string = 
  ## Converts to a ``COM_PING`` (mysql Client/Server Protocol) string.
  formatNoArgsComImpl Command.ping