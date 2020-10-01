import asyncdispatch
import macros
import uri
import ./conn
import ./conn_connection

type
  ## db pool
  AsyncPool* = ref object
    conns: seq[Connection]
    busy: seq[bool]

proc newAsyncPool*(
    host,
    user,
    password,
    database: string,
    num: int
  ): Future[AsyncPool] {.async.} =
  ## Create a new async pool of num connections.
  result = AsyncPool()
  for i in 0..<num:
    let conn = await open(host, user, password, database)
    result.conns.add conn
    result.busy.add false

proc newAsyncPool*(
    uriStr: string | Uri,
    num: int
  ): Future[AsyncPool] {.async.} =
  ## Create a new async pool of num connections.
  result = AsyncPool()
  for i in 0..<num:
    let conn = await open(uriStr)
    result.conns.add conn
    result.busy.add false

proc getFreeConnIdx*(pool: AsyncPool): Future[int] {.async.} =
  ## Wait for a free connection and return it.
  while true:
    for conIdx in 0..<pool.conns.len:
      if not pool.busy[conIdx]:
        pool.busy[conIdx] = true
        return conIdx
    await sleepAsync(1)

proc returnConn*(pool: AsyncPool, conIdx: int) =
  ## Make the connection as free after using it and getting results.
  pool.busy[conIdx] = false

proc close*(pool: AsyncPool) {.async.} = 
  for conn in pool.conns:
    await conn.close()