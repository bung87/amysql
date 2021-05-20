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
  # var pos: int = 1
  let n = int(conn.buf[conn.bufPos + 1])
  if n == 3:
    await conn.receivePacket()
    if isERRPacket(conn):
      raise parseErrorPacket(conn)
    return #pkt
  if n != 4:
    raise newException(ProtocolError,"caching sha2: Unknown packet for fast auth:" & $n)
  # full path
  debug "full path magic number:" & $n
  # raise newException(CatchableError, "Unimplemented")
  # if conn.secure # Sending plain password via secure connection (Localhost via UNIX socket or ssl)
  await conn.roundtrip(password & char(0))
  return 
  # if not conn.server_public_key:
  #   pkt = await roundtrip(conn, "2") 
  #   if not isExtraAuthDataPacket(conn):
  #     raise newException(ProtocolError,"caching sha2: Unknown packet for public key: "  & pkt)
  #   conn.server_public_key = pkt[1..pkt.high]
  # let data = sha2_rsa_encrypt(password, scrambleBuff, conn.server_public_key)
  # pkt = await roundtrip(conn, data)
  # return pkt