import asyncdispatch
import ../amysql
import db_common

type
  ## db pool
  AsyncPool* = ref object
    conns: seq[Connection]
    busy: seq[bool]

proc newAsyncPool*(
    connection,
    user,
    password,
    database: string,
    num: int
  ): AsyncPool =
  ## Create a new async pool of num connections.
  result = AsyncPool()
  for i in 0..<num:
    let conn = waitFor open(connection, user, password, database)
    result.conns.add conn
    result.busy.add false

proc getFreeConnIdx(pool: AsyncPool): Future[int] {.async.} =
  ## Wait for a free connection and return it.
  while true:
    for conIdx in 0..<pool.conns.len:
      if not pool.busy[conIdx]:
        pool.busy[conIdx] = true
        return conIdx
    await sleepAsync(1)

proc returnConn(pool: AsyncPool, conIdx: int) =
  ## Make the connection as free after using it and getting results.
  pool.busy[conIdx] = false

proc rawQuery*(
    pool: AsyncPool,
    query: string
  ): Future[ResultSet[string]] {.async.} =
  ## Runs the SQL getting results.
  let conIdx = await pool.getFreeConnIdx()
  result = await rawQuery(pool.conns[conIdx], query)
  pool.returnConn(conIdx)
