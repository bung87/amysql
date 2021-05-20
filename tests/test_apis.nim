import amysql
when defined(ChronosAsync):
  import chronos
else:
  import asyncdispatch
import unittest
import net

const database_name = "test"
const port: int = 3306
const host_name = "127.0.0.1"
const user_name = "test_user"
const pass_word = "12345678"
const ssl: bool = false
const verbose: bool = false

proc mainTests(conn: Connection): Future[void] {.async.} =
  discard await conn.selectDatabase(database_name)
  discard await conn.rawExec("drop table if exists num_tests")
  discard await conn.rawExec("create table num_tests (s text, u8 tinyint unsigned, s8 tinyint, u int unsigned, i int, b bigint)")

  # Insert values using the binary protocol
  let insrow = await conn.prepare("insert into `num_tests` (s, u8, s8, u, i, b) values (?, ?, ?, ?, ?, ?)")
  discard await conn.query(insrow, "one", 1, 1, 1, 1, 1)
  discard await conn.query(insrow, "max", 255, 127, 4294967295, 2147483647, 9223372036854775807'u64)
  discard await conn.query(insrow, "min", 0, -128, 0, -2147483648, (-9223372036854775807'i64 - 1))
  discard await conn.query(insrow, "foo", 128, -127, 256, -32767, -32768)
  await conn.finalize(insrow)

  checkpoint "rawQuery"
  let r1 = await conn.rawQuery("select s, u8, s8, u, i, b from num_tests order by u8 asc")
  check r1.columns.len() == 6
  check r1.rows.len() == 4

  checkpoint "rawExec"
  let r2 = await conn.rawExec("select s, u8, s8, u, i, b from num_tests order by u8 asc")
  check r2.rows.len == 0

  checkpoint "query"
  let r3 = await conn.query(sql"select s, u8, s8, u, i, b from num_tests order by u8 asc")
  check r3.columns.len() == 6
  check r3.rows.len() == 4

  checkpoint "query onlyFirst"
  let r4 = await conn.query(sql"select s, u8, s8, u, i, b from num_tests order by u8 asc", onlyFirst = true)
  check r4.rows.len  == 1

  checkpoint "getRow"
  let r5 = await conn.getRow(sql"select s, u8, s8, u, i, b from num_tests order by u8 asc")
  check r5 == @["min", "0", "-128", "0", "-2147483648", "-9223372036854775808"]

  checkpoint "getValue"
  let r6 = await conn.getValue(sql"select s, u8, s8, u, i, b from num_tests order by u8 asc")
  check r6 == "min"

proc runTests(): Future[void] {.async.} =
  let conn = await open(host_name,user_name,pass_word,database_name)
  await conn.mainTests()
  await conn.close()

test "test apis":
  waitFor(runTests())
