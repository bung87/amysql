import unittest
import os
import async_mysql, asyncdispatch, asyncnet
from nativesockets import AF_INET, SOCK_STREAM
import unittest
import net

const database_name = "test"
const port: int = 3306
const host_name = "localhost"
const ssl: bool = false

proc doTCPConnect(user_name, pass_word:string): Future[Connection] {.async.} =
  let sock = newAsyncSocket(AF_INET, SOCK_STREAM)
  await connect(sock, host_name, Port(port))
  if ssl:
    when defined(ssl):
      let ctx = newContext(verifyMode = CVerifyPeer)
      return await establishConnection(sock, user_name, database=database_name, password = pass_word, ssl=ctx)
  else:
    return await establishConnection(sock, user_name, database=database_name, password = pass_word)

suite "test connection auth methods":
  test "caching_sha2":
    if getEnv("USE_SHA2") == "true": 
      let conn = waitFor(doTCPConnect("sha2user","123456"))
      waitFor conn.close()