import amysql, asyncdispatch, os
import unittest
import net

const database_name = "test"
const port: int = 3306
const host_name = "localhost"
const user_name = "test_user"
const pass_word = "123456"
const ssl: bool = false
const verbose: bool = false

template assertEq(T: typedesc, got: untyped, expect: untyped, msg: string = "incorrect value") =
  check got == expect

proc numberTests(conn: Connection): Future[void] {.async.} =

  discard await conn.selectDatabase(database_name)
  discard await conn.rawQuery("drop table if exists float_test")
  # https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html
  # let deprecatedMD = conn.mariadb == false and conn.getDatabaseVersion >= Version("8.0.17")
  # if deprecatedMD:
  #   discard await conn.rawQuery("CREATE TABLE `float_test`(`fla` FLOAT,`flb` FLOAT,`dba` DOUBLE(53),`dbb` DOUBLE(53)")
  # else:
  discard await conn.rawQuery("CREATE TABLE `float_test`(`fla` FLOAT,`flb` FLOAT,`dba` DOUBLE(10,2),`dbb` DOUBLE(10,2))")

  # Insert values using the binary protocol
  let insrow = await conn.prepare("INSERT INTO `float_test` values (?,?,?,?)")
  discard await conn.query(insrow, 1.2'f32, 1.2'f32, 1.2'f64, 1.2'f64)
  await conn.finalize(insrow)

  # Read them back using the text protocol
  let r1 = await conn.rawQuery("SELECT * FROM `float_test`")

  assertEq(seq[string], r1.rows[0],
    @[ "1.2", "1.2", "1.20", "1.20" ])

  # Now read them back using the binary protocol
  let rdtab = await conn.prepare("SELECT * FROM `float_test`")
  let r2 = await conn.query(rdtab)

  assertEq(float32,  r2.rows[0][0], 1.2'f32)
  assertEq(float32,  r2.rows[0][1], 1.2'f32)
  assertEq(float64,  r2.rows[0][2], 1.2'f64)
  assertEq(float64,  r2.rows[0][3], 1.2'f64)

  await conn.finalize(rdtab)

  let rdtab2 = await conn.prepare("SELECT fla+flb, dba+dbb FROM `float_test`;")
  let r3 = await conn.query(rdtab2)
  assertEq(float32,  r3.rows[0][0], 2.4000000953674316'f32)
  assertEq(float64,  r3.rows[0][1], 2.40'f64)

  discard await conn.rawQuery("drop table `float_test`")

proc runTests(): Future[void] {.async.} =
  let conn = await open(host_name,user_name,pass_word,database_name)
  await conn.numberTests()
  await conn.close()

test "float and double":
  waitFor(runTests())
