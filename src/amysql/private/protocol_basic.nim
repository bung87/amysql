import strutils
import endians
import times

type
  # ProtocolError indicates we got something we don't understand. We might
  # even have lost framing, etc.. The connection should really be closed at this point.
  ProtocolError* = object of IOError

const
  LenEnc_16        = 0xFC
  LenEnc_24        = 0xFD
  LenEnc_64        = 0xFE

## Basic datatype packers/unpackers
## little endian
# Integers

proc scan16(buf: string, pos: int , p: pointer) {.inline.} =
  when system.cpuEndian == bigEndian:
    swapEndian16(p, buf[pos].addr)
  else:
    copyMem(p, buf[pos].unSafeAddr, 2)

proc put16*(buf: var string, p: pointer) {.inline.} =
  var arr:array[0..1, char]
  littleEndian16(addr arr, p)
  var str = newString(2)
  copyMem(str[0].addr, arr[0].addr, 2)
  buf.add str

proc scan32*(buf: string, pos: int , p: pointer) {.inline.} =
  when system.cpuEndian == bigEndian:
    swapEndian32(p, buf[pos].addr)
  else:
    copyMem(p, buf[pos].unSafeAddr, 4)

proc put32(buf: var string, p: pointer) {.inline.} =
  var arr:array[0..3, char]
  littleEndian32(addr arr, p)
  var str = newString(4)
  copyMem(str[0].addr, arr[0].addr, 4)
  buf.add str

proc scan64(buf: string, pos: int , p: pointer) {.inline.} =
  when system.cpuEndian == bigEndian:
    swapEndian64(p, buf[pos].addr)
  else:
    copyMem(p, buf[pos].unSafeAddr, 8)

proc put64(buf: var string, p: pointer) {.inline.} =
  var arr:array[0..7, char]
  littleEndian64(addr arr, p)
  var str = newString(8)
  copyMem(str[0].addr, arr[0].addr, 8)
  buf.add str

proc putU8*(buf: var string, val: uint8) {.inline.} =
  buf.add( char(val) )

proc putU8*(buf: var string, val: range[0..255]) {.inline.} =
  buf.add( char(val) )
  
proc scanU16*(buf: string, pos: int): uint16 =
  scan16(buf, pos, result.addr)

proc putU16*(buf: var string, val: uint16) =
  put16(buf, val.unSafeAddr)

proc scanU32*(buf: string, pos: int): uint32 =
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

proc scanFloat*(buf: string, pos: int): float32 =
  scan32(buf, pos, addr result)

proc scanDouble*(buf: string, pos: int): float64 =
  scan64(buf, pos, addr result)

proc scanU64*(buf: string, pos: int): uint64 =
  scan64(buf, pos, addr result)

proc putS64*(buf: var string, val: int64) =
  put64(buf, val.unSafeAddr)

proc putU64*(buf: var string, val: uint64) =
  put64(buf, val.unSafeAddr)

proc scanLenInt*(buf: string, pos: var int): int =
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


# Strings
proc scanNulString*(buf: string, pos: var int): string =
  result = ""
  while buf[pos] != char(0):
    result.add(buf[pos])
    inc(pos)
  inc(pos)

proc scanNulStringX*(buf: string, pos: var int): string =
  # scan null string limited to buf high
  result = ""
  while pos <= high(buf) and buf[pos] != char(0):
    result.add(buf[pos])
    inc(pos)
  inc(pos)

proc putNulString*(buf: var string, val: string) =
  buf.add(val)
  buf.add( char(0) )

proc scanLenStr*(buf: string, pos: var int): string =
  let slen = scanLenInt(buf, pos)
  if slen < 0:
    raise newException(ProtocolError, "lenenc-int: is 0x" & toHex(int(buf[pos]), 2))
  result = substr(buf, pos, pos+slen-1)
  pos = pos + slen

proc putLenStr*(buf: var string, val: string) =
  putLenInt(buf, val.len)
  buf.add(val)

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

  assert 0 == scanLenInt(buf, pos)
  assert 1    == scanLenInt(buf, pos)
  assert 250  == scanLenInt(buf, pos)
  assert 251  == scanLenInt(buf, pos)
  assert 252  == scanLenInt(buf, pos)
  assert 512  == scanLenInt(buf, pos)
  assert 640  == scanLenInt(buf, pos)
  assert 0x0FFFF == scanLenInt(buf, pos)
  assert 0x10000 ==  scanLenInt(buf, pos)
  assert 15715755 ==  scanLenInt(buf, pos)
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
