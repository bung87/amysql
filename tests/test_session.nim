import amysql
when defined(ChronosAsync):
  import chronos
else:
  import asyncdispatch
import unittest
import net
import strformat
import os

const database_name = "test"
const port: int = 3306
const host_name = "localhost"
const user_name = "test_user"
const pass_word = "12345678"
const ssl: bool = false
const verbose: bool = false

proc mainTests(conn: Connection): Future[void] {.async.} =
  echo $conn
  const DisableAutocommit = "SET autocommit = OFF"
  var r1:ResultSet[string]
  try:
    r1 = await conn.rawQuery( DisableAutocommit)
  except:
    discard
  check r1.status.sessionStateChanges[0].name == "autocommit"
  check r1.status.sessionStateChanges[0].value == "OFF"
  const useTest = "use test"
  var r2:ResultSet[string]
  try:
    r2 = await conn.rawQuery( useTest)
  except:
    discard
  check r2.status.sessionStateChanges[0].name == "test"

proc runTests(): Future[void] {.async.} =
  let conn = await open(host_name,user_name,pass_word,database_name)
  await conn.mainTests()
  await conn.close()

test "session":
  waitFor(runTests())
