when defined(ChronosAsync):
  import chronos
else:
  discard
import scorper
import std / [exitprocs]
import amysql

const database_name = "test"
const port: int = 3306
const host_name = "127.0.0.1"
const user_name = "test_user"
const pass_word = "12345678"
const ssl: bool = false
const verbose: bool = false

type AsyncCallback = proc (request: Request): Future[void] {.closure, gcsafe, raises: [].}

var conn{.threadvar.}:Connection

conn = waitFor amysql.open(host_name,user_name,pass_word,database_name)
# exitprocs.addExitProc proc() = waitFor conn.close()

proc queriesHandler(req: Request) {.async.} = 
  discard await conn.selectDatabase(database_name)
  discard await conn.rawExec("drop table if exists num_tests")
  discard await conn.rawExec("create table num_tests (s text, u8 tinyint unsigned, s8 tinyint, u int unsigned, i int, b bigint)")
  let insrow = await conn.prepare("insert into `num_tests` (s, u8, s8, u, i, b) values (?, ?, ?, ?, ?, ?)")
  discard await conn.query(insrow, "one", 1, 1, 1, 1, 1)
  discard await conn.query(insrow, "max", 255, 127, 4294967295, 2147483647, 9223372036854775807'u64)
  discard await conn.query(insrow, "min", 0, -128, 0, -2147483648, (-9223372036854775807'i64 - 1))
  discard await conn.query(insrow, "foo", 128, -127, 256, -32767, -32768)
  await conn.finalize(insrow)
  for i in 1 .. 2:
    try:
      discard await conn.rawQuery("select s, u8, s8, u, i, b from num_tests order by u8 asc")
    except Exception as e:
      echo $type(e),e.msg

  await req.resp("hello world")
  
let address = "127.0.0.1:8080"
let flags = {ReuseAddr}

waitFor serve(address, queriesHandler, flags)

