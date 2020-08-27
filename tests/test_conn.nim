import amysql, asyncdispatch
import unittest
import net
import strformat

const database_name = "test"
const port: int = 3306
const host_name = "127.0.0.1"
const user_name = "test_user"
const pass_word = "123456"
const ssl: bool = false
const verbose: bool = false

# The handling of localhost on Unix depends on the type of transport protocol.
# Connections using classic MySQL protocol handle localhost the same way as other MySQL clients,
# which means that localhost is assumed to be for socket-based connections.
# For connections using X Protocol, 
# the behavior of localhost differs in that it is assumed to represent the loopback address, 
# for example, IPv4 address 127.0.0.1.

proc getCurrentDatabase(conn: Connection): Future[string] {.async.} =
  let rslt = await conn.rawQuery("select database()")
  doAssert(len(rslt.columns) == 1, "wrong number of result columns")
  doAssert(len(rslt.rows) == 1, "wrong number of result rows")
  return rslt.rows[0][0]

proc connTest(): Future[Connection] {.async.} =
  let conn1 = await open(host_name,user_name,pass_word,database_name)
  let conn1db1 = await getCurrentDatabase(conn1)
  check conn1db1 == database_name
  let conn2 = await open(host_name,user_name,pass_word)
  let conn2db1 = await getCurrentDatabase(conn2)
  check conn2db1.len == 0
  discard await conn2.selectDatabase(database_name)
  let conn2db2 = await getCurrentDatabase(conn2)
  check conn2db2 == database_name

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

  await conn2.close()
  return conn1

proc runTests(): Future[void] {.async.} =
  let conn = await connTest()
  await conn.close()

suite "connnection":
  test "connnection with multiple instance":
    waitFor(runTests())
  
  test "dsn single param":
    ## Conversely, the second of the following lines is legal at runtime, but the first is not:
    ## SET GLOBAL max_allowed_packet=16M;
    ## SET GLOBAL max_allowed_packet=16*1024*1024
    let conn = waitFor amysql.open(fmt"mysqlx://{user_name}:{pass_word}@{host_name}/{database_name}?sql_mode=TRADITIONAL")
    waitFor conn.close()

  # A SET NAMES 'charset_name' statement is equivalent to these three statements:
  # SET character_set_client = charset_name;
  # SET character_set_results = charset_name;
  # SET character_set_connection = charset_name;

  test "dsn charset":
    let conn = waitFor amysql.open(fmt"mysqlx://{user_name}:{pass_word}@{host_name}/{database_name}?charset=utf8&sql_mode=TRADITIONAL")
    waitFor conn.close()

  test "dsn multiple charset":
    let conn = waitFor amysql.open(fmt"mysqlx://{user_name}:{pass_word}@{host_name}/{database_name}?charset=utf8mb4,utf8&sql_mode=TRADITIONAL")
    waitFor conn.close()
