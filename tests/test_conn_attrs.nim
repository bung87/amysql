import amysql, asyncdispatch
import unittest
import net
import tables
import logging
import strformat
import os

const database_name = "performance_schema"
const port: int = 3306
const host_name = "localhost"
const user_name = "test_user"
const pass_word = "12345678"
const ssl: bool = false
const verbose: bool = false

proc mainTests(conn: Connection): Future[void] {.async.} =
  # The session_account_connect_attrs table includes the attributes for connections
  # using the same user account as for the one querying the table. 
  # This is useful if you want to grant permission for a user to check
  #  the attributes for their own connections but not for other connections.
  # On the other hand, session_connect_attrs shows the attributes for all connections.
  # This is useful for the administrator to check the attributes for all users.
  if getEnv("TRAVIS") != "true":
    let r1 = await conn.rawQuery("select * from session_connect_attrs where ATTR_NAME=\"_client_name\"")
    # debug $conn
    # PROCESSLIST_ID ATTR_NAME ATTR_VALUE ORDINAL_POSITION
    check r1.rows[0][1] == "_client_name"
    check r1.rows[0][2] == "amysql"

proc runTests(): Future[void] {.async.} =
  let attrs = {"_client_name":"amysql"}.toTable
  let conn = await open(host_name,user_name,pass_word,database_name,attrs)
  await conn.mainTests()
  await conn.close()
  let connectAttrs = "connection-attributes=[_client_name=amysql,attr1=val1,attr2,attr3=]"
  let dsn = fmt"mysql://{user_name}:{pass_word}@{host_name}/{database_name}?{connectAttrs}"
  let conn2 = await amysql.open(dsn)
  await conn2.mainTests()
  await conn2.close()

test "connection attrs":
  waitFor(runTests())
