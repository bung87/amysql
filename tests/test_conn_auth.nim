import unittest
import os
import amysql
when defined(ChronosAsync):
  import chronos
else:
  import asyncdispatch
import unittest
import net

const database_name = "test"
const port: int = 3306
const host_name = "localhost"
const ssl: bool = false

suite "test connection auth methods":
  test "caching_sha2":
    if getEnv("USE_SHA2") == "true": 
      let conn = waitFor open(host_name,"sha2user","123456")
      waitFor conn.close()