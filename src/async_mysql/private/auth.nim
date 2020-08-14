import std/sha1, nimcrypto

const Sha1DigestSize = 20

proc `xor`(a: Sha1Digest, b: Sha1Digest): string =
  result = newStringOfCap(Sha1DigestSize)
  for i in 0..<Sha1DigestSize:
    let c = ord(a[i]) xor ord(b[i])
    add(result, chr(c))

proc `xor`(a: MDigest[256], b: MDigest[256]): string =
  result = newStringOfCap(32)
  for i in 0..<32:
    let c = ord(a.data[i]) xor ord(b.data[i])
    add(result, chr(c))

proc toString(h: sha1.SecureHash | Sha1Digest): string =
  ## convert sha1.SecureHash,Sha1Digest to limited length string(Sha1DigestSize:20)
  var bytes = cast[array[0 .. Sha1DigestSize-1, uint8]](h)
  result = newString(Sha1DigestSize)
  copyMem(result[0].addr, bytes[0].addr, bytes.len)

proc scramble_native_password*(scrambleBuff: string, password: string): string =
  let stage1 = sha1.secureHash(password)
  let stage2 = sha1.secureHash( stage1.toString )
  var ss = newSha1State()
  ss.update(scrambleBuff[0..<Sha1DigestSize])
  ss.update(stage2.toString)
  let stage3 = ss.finalize
  result = stage3 xor stage1.Sha1Digest

proc scramble_caching_sha2*(scrambleBuff: string, password: string): string =
  let p1 = sha256.digest(password)
  let p2 = sha256.digest($p1)
  let p3 = sha256.digest($p2 & scrambleBuff)
  result = p1 xor p3