when defined(ChronosAsync):
  import chronos
else:
  discard
import scorper
import amysql / async_pool

const database_name = "test"
const port: int = 3306
const host_name = "127.0.0.1"
const user_name = "test_user"
const pass_word = "12345678"
const ssl: bool = false
const verbose: bool = false

# var conn{.threadvar.}:Connection
# conn = waitFor amysql.open(host_name,user_name,pass_word,database_name)
var conn{.threadvar.}:AsyncPoolRef
conn = waitFor newAsyncPool(host_name,
  user_name,
  pass_word,
  database_name,2)

proc queriesHandler(req: Request) {.async.} =
  for i in 1 .. 2:
    try:
      discard await conn.rawQuery("select s, u8, s8, u, i, b from num_tests order by u8 asc")
    except Exception as e:
      echo $type(e),e.msg

  await req.resp("hello world")
  
let address = "127.0.0.1:8080"
let flags = {ReuseAddr}

waitFor serve(address, queriesHandler, flags)

