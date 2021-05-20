import unittest
when defined(ChronosAsync):
  import chronos
else:
  import asyncdispatch
import amysql/async_pool
import amysql
import logging

const database_name = "test"
const port: int = 3306
const host_name = "127.0.0.1"
const user_name = "test_user"
const pass_word = "12345678"
const ssl: bool = false
const verbose: bool = false


proc numberTests(pool: AsyncPool): Future[void] {.async.} =
  discard await pool.selectDatabase(database_name)
  discard await pool.rawExec("drop table if exists num_tests")
  discard await pool.rawExec("create table num_tests (s text, u8 tinyint unsigned, s8 tinyint, u int unsigned, i int, b bigint)")

  # Insert values using the binary protocol
  let conIdx = await pool.getFreeConnIdx()
  let conn = pool.getFreeConn(conIdx)
  debug $conn
  let insrow = await conn.prepare("insert into `num_tests` (s, u8, s8, u, i, b) values (?, ?, ?, ?, ?, ?)")
  discard await conn.query(insrow, "one", 1, 1, 1, 1, 1)
  discard await conn.query(insrow, "max", 255, 127, 4294967295, 2147483647, 9223372036854775807'u64)
  discard await conn.query(insrow, "min", 0, -128, 0, -2147483648, (-9223372036854775807'i64 - 1))
  discard await conn.query(insrow, "foo", 128, -127, 256, -32767, -32768)
  await conn.finalize(insrow)
  pool.returnConn(conIdx)

  # Read them back using the text protocol
  let r1 = await pool.rawQuery("select s, u8, s8, u, i, b from num_tests order by u8 asc")
  check r1.columns.len() == 6
  check r1.rows.len() == 4
  check r1.columns[0].name == "s"
  check r1.columns[5].name == "b"

  check r1.rows[0] == @[ "min", "0", "-128", "0", "-2147483648", "-9223372036854775808" ]
  check r1.rows[1] == @[ "one", "1", "1", "1", "1", "1" ]
  check r1.rows[2] == @[ "foo", "128", "-127", "256", "-32767", "-32768" ]
  check r1.rows[3] == @[ "max", "255", "127", "4294967295", "2147483647", "9223372036854775807" ]

  # Now read them back using the binary protocol
  let conIdx2 = await pool.getFreeConnIdx()
  let conn2 = pool.getFreeConn(conIdx)
  let rdtab = await conn2.prepare("select b, i, u, s, u8, s8 from num_tests order by i desc")
  let r2 = await conn2.query(rdtab)
  check r2.columns.len() == 6
  check r2.rows.len() == 4
  check r2.columns[0].name == "b"
  check r2.columns[5].name == "s8"

  check r2.rows[0][0] == 9223372036854775807'i64
  check r2.rows[0][0] == 9223372036854775807'u64
  check r2.rows[0][1] == 2147483647'i64
  check r2.rows[0][1] == 2147483647'u64
  check r2.rows[0][1] == 2147483647
  check r2.rows[0][1] == 2147483647'u
  check r2.rows[0][2] == 4294967295'u
  check r2.rows[0][2] == 4294967295'i64
  check r2.rows[0][2] == 4294967295'u64
  check r2.rows[0][3] == "max"
  check r2.rows[0][4] == 255
  check r2.rows[0][5] == 127

  check r2.rows[1][1] == 1
  check r2.rows[1][3] == "one"

  check r2.rows[2][0] == -32768
  check r2.rows[2][0] == -32768'i64
  check r2.rows[2][1] == -32767
  check r2.rows[2][1] == -32767'i64
  check r2.rows[2][2] == 256
  check r2.rows[2][3] == "foo"
  check r2.rows[2][4] == 128
  check r2.rows[2][5] == -127
  check r2.rows[2][5] == -127'i64

  check r2.rows[3][0] == ( -9223372036854775807'i64 - 1 )
  check r2.rows[3][1] == -2147483648
  check r2.rows[3][4] == 0
  check r2.rows[3][4] == 0'i64

  await conn.finalize(rdtab)
  pool.returnConn(conIdx)
  discard await pool.rawQuery("drop table `num_tests`")

proc runTests(): Future[void] {.async.} =
  var pool = await newAsyncPool(host_name,
    user_name,
    pass_word,
    database_name,2)
  await pool.numberTests()
  await pool.close()

test "async pool sql":
  waitFor(runTests())