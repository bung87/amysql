import unittest
import asyncdispatch
import amysql/async_pool

test "async pool":
  const database_name = "test"
  const port: int = 3306
  const host_name = "localhost"
  const ssl: bool = false
  const user_name = "test_user"
  const pass_word = "123456"
  var pool = waitFor newAsyncPool(host_name,
    user_name,
    pass_word,
    database_name,2)
  let rslt = waitFor pool.rawQuery("select database()")
  waitFor pool.close()