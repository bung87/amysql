proc hexstr(s: string): string =
  const HexChars = "0123456789abcdef"
  result = newString(s.len * 2)
  for pos, c in s:
    var n = ord(c)
    result[pos * 2 + 1] = HexChars[n and 0xF]
    n = n shr 4
    result[pos * 2] = HexChars[n]