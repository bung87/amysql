import unittest
import std/openssl
import amysql/private/auth
include ./utils
proc mysql_native_password(s: string,p: string): string = hexstr(scramble_native_password(s,p))

const testPublicKey = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAx+SOvNFYCZXM2RhRfaAs
UZAFXqfykaXa39AKAa8mTIVn6+GrdE4Ak4YuKOjTyy2J/GMFrEtD5TIk/ewdcH9d
X/iNV302BfUrvbMFCB9Qd9isxPuXf/47fRBoiyu2Qma2UsI0jXdTq9mTaqCJe1Pp
DFtHjKY7q5ZWT53sl5pgXYDLZunml4OgrrO2hAUhH7Ew07Nr2idipZrGpNEM275/
mX1UqwOReqDuajt7arjZcn7+bbZ32+kCJyRYOnesf8RBhQlUyg9oamyCscbvg+5X
wSt1CUw9IlFMW4n5uawEqba1t6YUsuBs73ltaFGcCAKu6WvquHXJlNGFVOudRtDf
qQIDAQAB
-----END PUBLIC KEY-----
"""

const testPrivateKey = """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDH5I680VgJlczZ
GFF9oCxRkAVep/KRpdrf0AoBryZMhWfr4at0TgCThi4o6NPLLYn8YwWsS0PlMiT9
7B1wf11f+I1XfTYF9Su9swUIH1B32KzE+5d//jt9EGiLK7ZCZrZSwjSNd1Or2ZNq
oIl7U+kMW0eMpjurllZPneyXmmBdgMtm6eaXg6Cus7aEBSEfsTDTs2vaJ2Klmsak
0Qzbvn+ZfVSrA5F6oO5qO3tquNlyfv5ttnfb6QInJFg6d6x/xEGFCVTKD2hqbIKx
xu+D7lfBK3UJTD0iUUxbifm5rASptrW3phSy4GzveW1oUZwIAq7pa+q4dcmU0YVU
651G0N+pAgMBAAECggEAAy3ReYE32iPvJra3IK4G8FBF1HJI5VGP0ElzTiWZZfir
K0N+TKBJnSdjKbwz5bML0wCn3ToOD3DoBgiKlF04qBsYzzW+x8v/x970GOO1pjZ5
3zZNMp/tzDikXlLQVT/FQpOJANpJOpUGaKg0tHsYH13HyXMnPuEtT/xn890t0if5
78B3zXWReK/QdVwxS37mLXH0rP6SHriFbaduepnylzMd+GQKGtOYPa5hR8Ge2xnz
njof8oVGfLVUQjzl0D5lAbo79DSOB+A3W/ItrMUJPuRcng/FSGZhp1CEEXWBNHgK
z6oIkvbuFO13Ssv4kmbJ13rdhV3696vPpKW3EjlW/wKBgQDr+rt8CN7sO16vJyqg
sA+CA9XSby0hDNeXEdlwDqU774i3m1OMV3Na0AisExTmW9Vl/p7gg/1ffZSkwe5q
BwbJeFoi0eFc2GN+O9NtSy0B+RqFc1cBv8akh7FJilsr7BK43FRhSesiZedMiXnk
L9LBgSJrA0sTAxz+2BHY9sIYdwKBgQDY2g4jlE0MIfA/g55VZolz300TN24yGQ1l
gkav2hx2fQ+2dqQIVwliPDah4YQRRkQzNlpT4RjSWIdRMGEPGjaS7TcQ5tURKqOM
S5uQj/B2Byh9WbuLMVwb9R2SiQxpuiiiqeHqIwmVl3RCyIgp7lhHJQkv7fIS+n+x
S1EsCyLw3wKBgCFMoGhAWDS9q/7LaBiEv6t5HxMy3Qts5H9M5YIKjwaGJqTCJmgH
bx/Be3fMuYZILzTVWVYVSgvPjMozE5GirDKB6TZAp6ySXHfTuWIS2acNn9B1FjII
/d75xuq5u2PXTE0Jyl5wL94KwzjprWml1fe4YUL4k/960PylQN988qbBAoGAOAZ+
SKr0DQJns8Epkx7evFUg8KoNilv2nhc+dAiB+grtmFKmppD2F68+3iwJlvz0eda9
F5a8dvvwWgWJzgETwHoRFAjPW5QMlR6jnoHRumy3BUu9rISgK4j0jvthfZ9fCEej
2n4a588tdmOg6lRt8lObQArV1BlsDD5NRnfKGzkCgYEA4kU4mVPwdpYY/oBvXeP7
shqkeOhLFWnuNWj7GQy7MA6BcYFCY2QS2tlAPHJQUH7xe48mYUkrfLARYxErtHmj
X/LSO21n8kqhHDknb2htYoKQqiGlvKox4VnPdChpvU5RfLHVpLg9GE+No8I0OWfr
T2oPCT6FpO5MYE9KmHYbexg=
-----END PRIVATE KEY-----
"""

proc decrypt(ciphertext: string): string =
  let privateKey = testPrivateKey
  let bio = BIO_new_mem_buf(privateKey[0].unsafeAddr, privateKey.len.cint)
  doAssert bio != nil
  defer: discard BIO_free(bio)
  let rsa = PEM_read_bio_RSAPrivateKey(bio, nil, nil, nil)
  doAssert rsa != nil
  defer: RSA_free(rsa)
  result = newString(RSA_size(rsa))
  let decryptedLen = RSA_private_decrypt(ciphertext.len.cint,
    cast[ptr uint8](ciphertext[0].unsafeAddr), cast[ptr uint8](result[0].addr),
    rsa, RSA_PKCS1_OAEP_PADDING)
  doAssert decryptedLen > 0
  result.setLen(decryptedLen)

suite "test password encryption":
  test "mysql_native_password":
    check mysql_native_password("L\\i{NQ09k2W>p<yk/DK+","foo") == "f828cd1387160a4c920f6c109d37285d281f7c85"
    check mysql_native_password("<G.N}OR-(~e^+VQtrao-","aaaaaaaaaaaaaaaaaaaabbbbbbbbbb") == "78797fae31fc733107e778ee36e124436761bddc"

  test "caching_sha2 RSA full authentication":
    const scramble = "01234567890123456789"
    const password = "test password"
    var plain = decrypt(sha2_rsa_encrypt(password, scramble, testPublicKey))
    check plain.len == password.len + 1
    for i in 0 ..< plain.len:
      plain[i] = char(plain[i].uint8 xor scramble[i mod scramble.len].uint8)
    check plain == password & char(0)

  test "caching_sha2 rejects an invalid public key":
    expect ValueError:
      discard sha2_rsa_encrypt("password", "01234567890123456789", "invalid")