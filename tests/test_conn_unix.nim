import async_mysql, asyncdispatch
import unittest
import net
import os

proc findSocketPath():string = 
  let paths = @["/var/lib/mysql/mysql.sock","/var/run/mysqld/mysql.sock","/tmp/mysql.sock"]
  for p in paths:
    if fileExists(p):
      return p

const database_name = "test"
const port: int = 3306
const host_name = findSocketPath()
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
  if host_name.len > 0:
    echo "Connecting with socket path: " & host_name
  let conn1 = await open(host_name,user_name,pass_word,database_name)
  echo "Checking current database is correct"
  let conn1db1 = await getCurrentDatabase(conn1)
  check conn1db1 == database_name

  let conn2 = await open(host_name,user_name,pass_word)
  let conn2db1 = await getCurrentDatabase(conn2)
  check conn2db1.len == 0
  discard await conn2.selectDatabase(database_name)
  let conn2db2 = await getCurrentDatabase(conn2)
  check conn2db2 == database_name

  echo "Checking TIDs (", conn1.thread_id, ", ", conn2.thread_id, ")"
  let rslt = await conn1.rawQuery("show processlist");
  var saw_conn1 = false
  var saw_conn2 = false
  for row in rslt.rows:
    if row[0] == $(conn1.thread_id):
      check saw_conn1 == false
      saw_conn1 = true
    if row[0] == $(conn2.thread_id):
      check saw_conn2 == false
      saw_conn2 = true
  check saw_conn1
  check saw_conn2
 
  echo "Closing second connection"
  await conn2.close()
  return conn1

proc runTests(): Future[void] {.async.} =
  let conn = await connTest()
  await conn.close()

test "connnection with unix socket":
  when defined(posix) and host_name.len > 0:
    waitFor(runTests())
  else:
    discard