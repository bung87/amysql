import async_mysql, asyncdispatch
import unittest
import net
import asyncnet
const database_name = "test"
const port: int = 3306
const host_name = "localhost"
const user_name = "test_user"
const pass_word = "123456"
const ssl: bool = false
const verbose: bool = false


proc getCurrentDatabase(conn: Connection): Future[string] {.async.} =
  let rslt = await conn.rawQuery("select database()")
  doAssert(len(rslt.columns) == 1, "wrong number of result columns")
  doAssert(len(rslt.rows) == 1, "wrong number of result rows")
  return rslt.rows[0][0]

proc connTest(): Future[Connection] {.async.} =
  echo "Connecting (with initial db: ", database_name, ")"
  let sock = newAsyncSocket(AF_INET, SOCK_STREAM)
  await connect(sock, host_name, Port(port))
  when defined(ssl):
    let ctx = newContext(verifyMode = CVerifyPeer)
    let conn1 = await establishConnection(sock,user_name,pass_word,database_name,ctx)
  else:
    let conn1 = await establishConnection(sock,user_name,pass_word,database_name)
  echo "Checking current database is correct"
  let conn1db1 = await getCurrentDatabase(conn1)
  check conn1db1 == database_name

  return conn1

proc runTests(): Future[void] {.async.} =
  let conn = await connTest()
  await conn.close()

test "connnection":
  waitFor(runTests())