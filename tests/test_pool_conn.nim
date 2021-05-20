import amysql
when defined(ChronosAsync):
  import chronos
else:
  import asyncdispatch
import unittest
import net
import strformat
import amysql/db_pool
import times

const database_name = "test"
const port: int = 3306
const host_name = "127.0.0.1"
const user_name = "test_user"
const pass_word = "12345678"


proc mainTest(){.async.} = 
  let pool = waitFor newDBPool(fmt"mysql://{user_name}:{pass_word}@{host_name}/{database_name}?minPoolSize=2&maxPoolSize=4")

  discard await pool.rawQuery("drop table if exists test_dt")
  discard await pool.rawQuery("CREATE TABLE test_dt(col DATE NOT NULL)")
  let r = await pool.rawQuery("SHOW TABLES LIKE 'test_dt'")
  check r.rows.len == 1
  # Insert values using the binary protocol
  let insrow = await pool.prepare("INSERT INTO test_dt (col) VALUES (?),(?)")
  let d1 = initDate(1,1.Month,2020)
  let d2 = initDate(31,12.Month,2020)
  discard await pool.query(insrow, d1, d2)
  await pool.finalize(insrow)

  # Read them back using the text protocol
  let r1 = await pool.rawQuery("SELECT * FROM test_dt")
  check r1.rows[0][0] == "2020-01-01"
  check r1.rows[1][0] == "2020-12-31"

  # Now read them back using the binary protocol
  let rdtab = await pool.prepare("SELECT * FROM test_dt")
  let r2 = await pool.query(rdtab)

  check r2.rows[0][0] == d1
  check r2.rows[1][0] == d2

  await pool.finalize(rdtab)

  discard await pool.rawQuery("drop table test_dt")
  await pool.close()

suite "pool connnection":
  test "connnection":
    waitFor mainTest()
   
  