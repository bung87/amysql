import unittest
include amysql
import strformat
const database_name = "test"
const port: int = 3306
const host_name = "localhost"
const user_name = "test_user"
const pass_word = "12345678"
const ssl: bool = false
const verbose: bool = false

suite "test mysql datetime":
  test "zero datetime $ proc":
    let r = ResultValue(typ: rvtDateTime,datetimeVal:DateTime() )
    check $r == "0000-00-00"
  test "zero datetime converter through compare to string":
    let r = ResultValue(typ: rvtDateTime,datetimeVal:DateTime() )
    check r == "0000-00-00"
  test "insert and read zero datetime":
    # remove default NO_ZERO_DATE
    let sqlmode = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
    let conn = waitFor open(host_name,user_name,pass_word,database_name)
    discard waitFor conn.rawQuery("drop table if exists test_dt")

    discard waitFor conn.rawQuery(fmt"SET sql_mode='{sqlmode}'")

    discard waitFor conn.rawQuery("CREATE TABLE test_dt(col DATETIME NOT NULL)")
    let insrow = waitFor conn.prepare("INSERT INTO test_dt (col) VALUES (?)")
    let d1 = DateTime()
    let r = waitFor conn.query(insrow, d1 )
    check r.status.warningCount == 0
    waitFor conn.finalize(insrow)
    let v = waitFor conn.getValue(sql"select col from test_dt")
    check v == "0000-00-00 00:00:00"

    let v2 = waitFor conn.prepare("select col from test_dt")
    let r2 = waitFor conn.query(v2)
    check default(DateTime) == r2.rows[0][0]
    discard waitFor conn.rawQuery("drop table `test_dt`")
    waitFor conn.close()