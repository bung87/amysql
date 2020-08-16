import async_mysql, asyncdispatch, asyncnet, os
from nativesockets import AF_INET, SOCK_STREAM
import unittest
import net

const database_name = "test"
const port: int = 3306
const host_name = "localhost"
const user_name = "test_user"
const pass_word = "123456"
const ssl: bool = false
const verbose: bool = false

proc doTCPConnect(dbn: string = ""): Future[Connection] {.async.} =
  let sock = newAsyncSocket(AF_INET, SOCK_STREAM)
  await connect(sock, host_name, Port(port))
  if ssl:
    when defined(ssl):
      let ctx = newContext(verifyMode = CVerifyPeer)
      return await establishConnection(sock, user_name, database=dbn, password = pass_word, ssl=ctx)
  else:
    return await establishConnection(sock, user_name, database=dbn, password = pass_word)

proc getCurrentDatabase(conn: Connection): Future[string] {.async.} =
  let rslt = await conn.rawQuery("select database()")
  doAssert(len(rslt.columns) == 1, "wrong number of result columns")
  doAssert(len(rslt.rows) == 1, "wrong number of result rows")
  return rslt.rows[0][0]

proc connTest(): Future[Connection] {.async.} =
  echo "Connecting (with initial db: ", database_name, ")"
  let conn1 = await doTCPConnect(dbn = database_name)
  echo "Checking current database is correct"
  let conn1db1 = await getCurrentDatabase(conn1)
  if conn1db1 != database_name:
    echo "FAIL (actual db: ", $conn1db1, ")"
  echo "Connecting (without initial db)"
  let conn2 = await doTCPConnect()
  let conn2db1 = await getCurrentDatabase(conn2)
  if conn2db1.len > 0:
    echo "FAIL (db should be NULL, is: ", $conn2db1, ")"
  discard await conn2.selectDatabase(database_name)
  let conn2db2 = await getCurrentDatabase(conn2)
  if conn2db2 != database_name:
    echo "FAIL (db should be: ", database_name, " is: ", conn2db2, ")"
  echo "Checking TIDs (", conn1.thread_id, ", ", conn2.thread_id, ")"
  let rslt = await conn1.rawQuery("show processlist");
  var saw_conn1 = false
  var saw_conn2 = false
  for row in rslt.rows:
    if row[0] == $(conn1.thread_id):
      doAssert(saw_conn1 == false, "Multiple rows with conn1's TID")
      saw_conn1 = true
    if row[0] == $(conn2.thread_id):
      doAssert(saw_conn2 == false, "Multiple rows with conn1's TID")
      saw_conn2 = true
  doAssert(saw_conn1, "Didn't see conn1's TID")
  doAssert(saw_conn2, "Didn't see conn2's TID")
  echo "Closing second connection"
  await conn2.close()
  return conn1

proc runTests(): Future[void] {.async.} =
  let conn = await connTest()
  await conn.close()

waitFor(runTests())