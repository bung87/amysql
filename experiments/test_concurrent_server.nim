when defined(ChronosAsync):
  import chronos
else:
  discard
import scorper
import ../ src / amysql 
import  ../ src / amysql / async_pool
import std / exitProcs

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
# on macOS: sudo launchctl limit maxfiles 10240 unlimited
# sudo sysctl -w kern.maxfiles=20480
# ulimit -n 8192
# [mysqld]
# thread_cache_size = 10240
# max_user_connections = 10240
# mysqlx_max_connections = 10240
# max_connections = 10240
# open_files_limit=34484
# restart your mysql server
# https://serverfault.com/questions/15564/where-are-the-default-ulimits-specified-on-os-x-10-5


echo getMaxOpenFiles()
# setMaxOpenFiles(8192)
var pool{.threadvar.}:AsyncPoolRef
pool = waitFor newAsyncPool(host_name,user_name,pass_word,database_name,2)
exitProcs.addExitProc proc() = waitFor pool.close()
echo "pool inited"
discard waitFor pool.rawExec("drop table if exists num_tests")
discard waitFor pool.rawExec("create table num_tests ( i int)")

proc queriesHandler(req: Request) {.async.} =
  
  for i in 1 .. 2:
    try:
      discard await pool.getRow(sql"select * from num_tests")
    except Exception as e:
      echo $type(e),e.msg
  
  await req.resp("hello world")
  
let address = "127.0.0.1:8080"
let flags = {ReuseAddr}

waitFor serve(address, queriesHandler, flags)

