when defined(ChronosAsync):
  import chronos
else:
  discard
import scorper
import amysql / async_pool
import cpuinfo, math
const database_name = "test"
const port: int = 3306
const host_name = "127.0.0.1"
const user_name = "test_user"
const pass_word = "12345678"
const ssl: bool = false
const verbose: bool = false

# var conn{.threadvar.}:Connection
# conn = waitFor amysql.open(host_name,user_name,pass_word,database_name)
# const poolSizeA = 512 #nextPowerOfTwo(cpuinfo.countProcessors())
# on macOS: launchctl limit maxfiles 10240 unlimited
# sudo sysctl -w kern.maxfiles=20480
# [mysqld]
# max_connections = 1000
# restart your mysql server
# https://serverfault.com/questions/15564/where-are-the-default-ulimits-specified-on-os-x-10-5

var conn{.threadvar.}:AsyncPoolRef
conn = waitFor newAsyncPool(host_name,user_name,pass_word,database_name,512)
echo "pool inited"
discard waitFor conn.rawExec("drop table if exists num_tests")
discard waitFor conn.rawExec("create table num_tests ( i int)")

proc queriesHandler(req: Request) {.async.} =
  
  for i in 1 .. 2:
    try:
      discard await conn.rawQuery("select * from num_tests")
    except Exception as e:
      echo $type(e),e.msg

  await req.resp("hello world")
  
let address = "127.0.0.1:8080"
let flags = {ReuseAddr}

waitFor serve(address, queriesHandler, flags)

