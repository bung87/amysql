import async_mysql, asyncdispatch, asyncnet, os, parseutils
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
  let rslt = await conn.textQuery("select database()")
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
  let rslt = await conn1.textQuery("show processlist");
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

template assertEq(T: typedesc, got: untyped, expect: untyped, msg: string = "incorrect value") =
  check got == expect

proc numberTests(conn: Connection): Future[void] {.async.} =
  echo "Setting up table for numeric tests..."
  discard await conn.textQuery("drop table if exists num_tests")
  discard await conn.textQuery("create table num_tests (s text, u8 tinyint unsigned, s8 tinyint, u int unsigned, i int, b bigint)")

  echo "Testing numeric parameters"
  # Insert values using the binary protocol
  let insrow = await conn.prepareStatement("insert into `num_tests` (s, u8, s8, u, i, b) values (?, ?, ?, ?, ?, ?)")
  discard await conn.preparedQuery(insrow, "one", 1, 1, 1, 1, 1)
  discard await conn.preparedQuery(insrow, "max", 255, 127, 4294967295, 2147483647, 9223372036854775807'u64)
  discard await conn.preparedQuery(insrow, "min", 0, -128, 0, -2147483648, (-9223372036854775807'i64 - 1))
  discard await conn.preparedQuery(insrow, "foo", 128, -127, 256, -32767, -32768)
  await conn.closeStatement(insrow)

  # Read them back using the text protocol
  let r1 = await conn.textQuery("select s, u8, s8, u, i, b from num_tests order by u8 asc")
  assertEq(int, r1.columns.len(), 6, "column count")
  assertEq(int, r1.rows.len(), 4, "row count")
  assertEq(string, r1.columns[0].name, "s")
  assertEq(string, r1.columns[5].name, "b")

  assertEq(seq[string], r1.rows[0],
    @[ "min", "0", "-128", "0", "-2147483648", "-9223372036854775808" ])
  assertEq(seq[string], r1.rows[1],
    @[ "one", "1", "1", "1", "1", "1" ])
  assertEq(seq[string], r1.rows[2],
    @[ "foo", "128", "-127", "256", "-32767", "-32768" ])
  assertEq(seq[string], r1.rows[3],
    @[ "max", "255", "127", "4294967295", "2147483647", "9223372036854775807" ])

  # Now read them back using the binary protocol
  echo "Testing numeric results"
  let rdtab = await conn.prepareStatement("select b, i, u, s, u8, s8 from num_tests order by i desc")
  let r2 = await conn.preparedQuery(rdtab)
  assertEq(int, r2.columns.len(), 6, "column count")
  assertEq(int, r2.rows.len(), 4, "row count")
  assertEq(string, r2.columns[0].name, "b")
  assertEq(string, r2.columns[5].name, "s8")

  assertEq(int64,  r2.rows[0][0], 9223372036854775807'i64)
  assertEq(uint64, r2.rows[0][0], 9223372036854775807'u64)
  assertEq(int64,  r2.rows[0][1], 2147483647'i64)
  assertEq(uint64, r2.rows[0][1], 2147483647'u64)
  assertEq(int,    r2.rows[0][1], 2147483647)
  assertEq(uint,   r2.rows[0][1], 2147483647'u)
  assertEq(uint,   r2.rows[0][2], 4294967295'u)
  assertEq(int64,  r2.rows[0][2], 4294967295'i64)
  assertEq(uint64, r2.rows[0][2], 4294967295'u64)
  assertEq(string, r2.rows[0][3], "max")
  assertEq(int,    r2.rows[0][4], 255)
  assertEq(int,    r2.rows[0][5], 127)

  assertEq(int,    r2.rows[1][1], 1)
  assertEq(string, r2.rows[1][3], "one")

  assertEq(int,    r2.rows[2][0], -32768)
  assertEq(int64,  r2.rows[2][0], -32768'i64)
  assertEq(int,    r2.rows[2][1], -32767)
  assertEq(int64,  r2.rows[2][1], -32767'i64)
  assertEq(int,    r2.rows[2][2], 256)
  assertEq(string, r2.rows[2][3], "foo")
  assertEq(int,    r2.rows[2][4], 128)
  assertEq(int,    r2.rows[2][5], -127)
  assertEq(int64,  r2.rows[2][5], -127'i64)

  assertEq(int64,  r2.rows[3][0], ( -9223372036854775807'i64 - 1 ))
  assertEq(int,    r2.rows[3][1], -2147483648)
  assertEq(int,    r2.rows[3][4], 0)
  assertEq(int64,  r2.rows[3][4], 0'i64)

  await conn.closeStatement(rdtab)
  discard await conn.textQuery("drop table `num_tests`")

proc runTests(): Future[void] {.async.} =
  let conn = await connTest()
  await conn.numberTests()
  await conn.close()

when defined(test):
  runInternalTests()
waitFor(runTests())
echo "Done"
