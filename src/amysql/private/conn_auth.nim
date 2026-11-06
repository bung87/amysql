import ../conn
import ./errors
import ./auth
import ./protocol
when defined(ChronosAsync):
  import chronos/[asyncloop, asyncsync, handles, transport, timer]
  import times except milliseconds,Duration,toParts,DurationZero,initDuration
  const DurationZero = default(Duration)
else:
  import asyncdispatch,times
import strutils
import logging

when defined(release):  setLogFilter(lvlInfo)

proc caching_sha2_password_auth*(conn:Connection,scrambleBuff, password: string) {.async.} =
  # pkt 
  # 1 status 0x01
  # 2 auth_method_data (string.EOF) -- extra auth-data beyond the initial challenge
  const ErrorMsg = "caching sha2: Unknown packet for fast auth:$1" 
  if password.len == 0:
    await conn.roundtrip("")
    return
  if conn.isAuthSwitchRequestPacket():
    let responseAuthSwitch = conn.parseAuthSwitchPacket()
    let authData = scramble_caching_sha2(responseAuthSwitch.pluginData, password)
    await conn.roundtrip(authData)
  if not conn.isExtraAuthDataPacket():
    raise newException(ProtocolError,ErrorMsg.format cast[string](conn.buf))
  
  # magic numbers:
  # 2 - request public key
  # 3 - fast auth succeeded
  # 4 - need full auth
  let n = int(conn.buf[conn.bufPos + 1])
  if n == 3:
    await conn.receivePacket()
    conn.resetPacketLen()
    if isERRPacket(conn):
      raise parseErrorPacket(conn)
    if not isOKPacket(conn):
      raise newException(ProtocolError, "caching sha2: Expected OK after fast auth")
    return
  if n != 4:
    raise newException(ProtocolError,"caching sha2: Unknown packet for fast auth:" & $n)

  debug "full path magic number:" & $n
  if conn.secure:
    # TLS and Unix sockets protect the NUL-terminated clear-text password.
    await conn.roundtrip(password & char(0))
  else:
    await conn.roundtrip("\x02")
    if not conn.isExtraAuthDataPacket():
      raise newException(ProtocolError, "caching sha2: Expected server public key")
    if conn.fullPacketLen <= conn.bufPos + 1:
      raise newException(ProtocolError, "caching sha2: Empty server public key")
    let publicKey = cast[string](conn.buf[conn.bufPos + 1 ..< conn.fullPacketLen])
    let data = sha2_rsa_encrypt(password, scrambleBuff, publicKey)
    await conn.roundtrip(data)

  if not isOKPacket(conn):
    raise newException(ProtocolError, "caching sha2: Expected OK after full auth")