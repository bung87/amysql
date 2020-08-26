import amysql, asyncdispatch
import unittest
import net
import times
const database_name = "test"
const port: int = 3306
const host_name = "localhost"
const user_name = "test_user"
const pass_word = "123456"
const ssl: bool = false
const verbose: bool = false

proc numberTests(conn: Connection): Future[void] {.async.} =
  discard await conn.selectDatabase(database_name)
  discard await conn.rawQuery("drop table if exists test_dt")
  discard await conn.rawQuery("CREATE TABLE test_dt(start_at TIME,end_at TIME)")

  # Insert values using the binary protocol
  let insrow = await conn.prepare("INSERT INTO test_dt (start_at,end_at) VALUES (?,?)")
  let d1 = initDuration(hours=8)
  let d2 = initDuration(hours=10)
  discard await conn.query(insrow, d1, d2)
  await conn.finalize(insrow)

  # Read them back using the text protocol
  let r1 = await conn.rawQuery("SELECT * FROM test_dt")
  check r1.rows[0][0] == "08:00:00"
  check r1.rows[0][1] == "10:00:00"

  # Now read them back using the binary protocol
  let rdtab = await conn.prepare("SELECT * FROM test_dt")
  let r2 = await conn.query(rdtab)

  check r2.rows[0][0] == d1
  check r2.rows[0][1] == d2

  await conn.finalize(rdtab)

  discard await conn.rawQuery("drop table test_dt")

proc runTests(): Future[void] {.async.} =
  let conn = await open(host_name,user_name,pass_word,database_name)
  await conn.numberTests()
  await conn.close()

test "time":
  waitFor(runTests())
