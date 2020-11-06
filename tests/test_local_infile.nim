import amysql, asyncdispatch
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
  # select * from e into outfile "/data/mysql/e.sql";
  # grant FILE on *.* to 'test_user'@'localhost'
  # FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\'
  # LINES TERMINATED BY '\n' STARTING BY ''
  discard await conn.rawQuery("drop table if exists person")
  const CreateTable = """
  CREATE TABLE IF NOT EXISTS `person`(
   `id` INT UNSIGNED AUTO_INCREMENT,
   `fname` VARCHAR(100) NOT NULL,
   `lname` VARCHAR(40) NOT NULL,
   PRIMARY KEY ( `id` )
  ) ENGINE=MyISAM DEFAULT CHARSET=utf8
  """
  const EnableLocalInfileData = "SET GLOBAL local_infile = true"
  discard await conn.tryQuery(SqlQuery EnableLocalInfileData)
  discard await conn.rawQuery(CreateTable)
  let filename = currentSourcePath.parentDir / "localinfile.csv"
  let r = await conn.rawQuery(fmt"LOAD DATA LOCAL INFILE '{filename}' INTO TABLE person")

proc runTests(): Future[void] {.async.} =
  let conn = await open(host_name,user_name,pass_word,database_name)
  await conn.mainTests()
  await conn.close()

test "local infile":
  waitFor(runTests())
