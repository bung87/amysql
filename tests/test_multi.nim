import amysql
when defined(ChronosAsync):
  import chronos
else:
  import asyncdispatch
import unittest
import net

# import regex
# import parsesql
# import strutils

const database_name = "test"
const port: int = 3306
const host_name = "127.0.0.1"
const user_name = "test_user"
const pass_word = "12345678"
const ssl: bool = false
const verbose: bool = false

proc mainTests(conn: Connection): Future[void] {.async.} =
  let q = sql"""
    drop table if exists num_tests;
    create table num_tests (s text, u8 tinyint unsigned, s8 tinyint, u int unsigned, i int, b bigint);
    insert into `num_tests` (s, u8, s8, u, i, b) values (?, ?, ?, ?, ?, ?);
    select s, u8, s8, u, i, b from num_tests order by u8 asc;
    """
  # const R = re"""([^;]*?((\'.*?\')|(".*?"))?)*?(;\s*|\s*$)"""
  # let s = dedent q.string
  # for m in s.findAll(R):
  #   var q = s[m.boundaries]
  #   removePrefix(q)
  #   removeSuffix(q)
  #   echo repr q
  #   let node = parseSQL(q)
  #   echo treeRepr(node)
  let r = await conn.query(q, false, "foo", "128", "-127", "256", "-32767", "-32768")
  check r.rows == @[@[ "foo", "128", "-127", "256", "-32767", "-32768" ]]

proc runTests(): Future[void] {.async.} =
  let conn = await open(host_name,user_name,pass_word,database_name)
  await conn.mainTests()
  await conn.close()

test "multi statements and multi results":
  waitFor(runTests())
