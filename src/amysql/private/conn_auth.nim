import ../conn
import ./errors
import ./auth
import ./protocol
import asyncdispatch
import logging

when defined(release):  setLogFilter(lvlInfo)

proc caching_sha2_password_auth*(conn:Connection, pkt, scrambleBuff, password: string): Future[string] {.async.} =
  # pkt 
  # 1 status 0x01
  # 2 auth_method_data (string.EOF) -- extra auth-data beyond the initial challenge
  if password.len == 0:
    return await conn.roundtrip("")
  var pkt = pkt
  var pktLen:int
  if pkt.isAuthSwitchRequestPacket():
    let responseAuthSwitch = conn.parseAuthSwitchPacket(pkt)
    let authData = scramble_caching_sha2(responseAuthSwitch.pluginData, password)
    pkt = await conn.roundtrip(authData)
  if not pkt.isExtraAuthDataPacket:
    raise newException(ProtocolError,"caching sha2: Unknown packet for fast auth:" & pkt)
  
  # magic numbers:
  # 2 - request public key
  # 3 - fast auth succeeded
  # 4 - need full auth
  # var pos: int = 1
  let n = int(pkt[1])
  if n == 3:
    (pkt, pktLen) = await conn.receivePacket()
    if isERRPacket(pkt):
      raise parseErrorPacket(pkt)
    return pkt
  if n != 4:
    raise newException(ProtocolError,"caching sha2: Unknown packet for fast auth:" & $n)
  # full path
  debug "full path magic number:" & $n
  # raise newException(CatchableError, "Unimplemented")
  # if conn.secure # Sending plain password via secure connection (Localhost via UNIX socket or ssl)
  return await conn.roundtrip(password & char(0))
  # if not conn.server_public_key:
  #   pkt = await roundtrip(conn, "2") 
  #   if not isExtraAuthDataPacket(pkt):
  #     raise newException(ProtocolError,"caching sha2: Unknown packet for public key: "  & pkt)
  #   conn.server_public_key = pkt[1..pkt.high]
  # let data = sha2_rsa_encrypt(password, scrambleBuff, conn.server_public_key)
  # pkt = await roundtrip(conn, data)
  # return pkt