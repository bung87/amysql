import strutils

type
  # ProtocolError indicates we got something we don't understand. We might
  # even have lost framing, etc.. The connection should really be closed at this point.
  ProtocolError* = object of IOError

const
  LenEnc_16        = 0xFC
  LenEnc_24        = 0xFD
  LenEnc_64        = 0xFE
## ######################################################################
##
## Basic datatype packers/unpackers

# Integers

proc scanU32*(buf: string, pos: int): uint32 =
  result = uint32(buf[pos]) + `shl`(uint32(buf[pos+1]), 8'u32) + (uint32(buf[pos+2]) shl 16'u32) + (uint32(buf[pos+3]) shl 24'u32)

proc putU32*(buf: var string, val: uint32) =
  buf.add( char( val and 0xff ) )
  buf.add( char( (val shr 8)  and 0xff ) )
  buf.add( char( (val shr 16) and 0xff ) )
  buf.add( char( (val shr 24) and 0xff ) )

proc scanU16*(buf: string, pos: int): uint16 =
  result = uint16(buf[pos]) + (uint16(buf[pos+1]) shl 8'u16)

proc putU16*(buf: var string, val: uint16) =
  buf.add( char( val and 0xFF ) )
  buf.add( char( (val shr 8) and 0xFF ) )

proc putU8*(buf: var string, val: uint8) {.inline.} =
  buf.add( char(val) )

proc putU8*(buf: var string, val: range[0..255]) {.inline.} =
  buf.add( char(val) )

proc scanU64*(buf: string, pos: int): uint64 =
  let l32 = scanU32(buf, pos)
  let h32 = scanU32(buf, pos+4)
  return uint64(l32) + ( (uint64(h32) shl 32 ) )

proc putS64*(buf: var string, val: int64) =
  let compl: uint64 = cast[uint64](val)
  buf.putU32(uint32(compl and 0xFFFFFFFF'u64))
  buf.putU32(uint32(compl shr 32))

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

proc putLenInt*(buf: var string, val: int) =
  if val < 0:
    raise newException(ProtocolError, "trying to send a negative lenenc-int")
  elif val < 251:
    buf.add( char(val) )
  elif val < 65536:
    buf.add( char(LenEnc_16) )
    buf.add( char( val and 0xFF ) )
    buf.add( char( (val shr 8) and 0xFF ) )
  elif val <= 0xFFFFFF:
    buf.add( char(LenEnc_24) )
    buf.add( char( val and 0xFF ) )
    buf.add( char( (val shr 8) and 0xFF ) )
    buf.add( char( (val shr 16) and 0xFF ) )
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

proc hexstr*(s: string): string =
  const HexChars = "0123456789abcdef"
  result = newString(s.len * 2)
  for pos, c in s:
    var n = ord(c)
    result[pos * 2 + 1] = HexChars[n and 0xF]
    n = n shr 4
    result[pos * 2] = HexChars[n]

when isMainModule or defined(test):
 
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
