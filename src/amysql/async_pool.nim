
import asyncdispatch
import asyncnet
import macros
import uri
import ../amysql
import ../amysql/private/format
import ./async_varargs

type
  ## db pool
  AsyncPool* = ref object
    conns: seq[Connection]
    busy: seq[bool]
    hostname*, username*, password*, database*: string

proc newAsyncPool*(
    host,
    user,
    password,
    database: string,
    num: int
  ): Future[AsyncPool] {.async.} =
  ## Create a new async pool of num connections.
  result = AsyncPool()
  result.hostname = host
  result.username = user
  result.password = password
  result.database = database
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
  let uri:Uri = when uriStr is string: parseUri(uriStr) else: uriStr
  result.hostname = uri.hostname
  result.username = uri.username
  result.password = uri.password
  result.database = uri.path[ 1 .. uri.path.high ]
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

proc getFreeConn*(pool: AsyncPool, conIdx: int): Future[Connection] {.async.} =
  if pool.conns[conIdx].socket.isClosed():
    pool.conns[conIdx] = await open(pool.hostname, pool.username, pool.password, pool.database)
  result = pool.conns[conIdx]

proc returnConn*(pool: AsyncPool, conIdx: int) =
  ## Make the connection as free after using it and getting results.
  pool.busy[conIdx] = false

proc close*(pool: AsyncPool) {.async.} = 
  for conn in pool.conns:
    await conn.close()

proc query*(pool: AsyncPool, pstmt: SqlPrepared, params: openarray[static[SqlParam]]): Future[void] {.async.}=
  let conIdx = await pool.getFreeConnIdx()
  let conn = await pool.getFreeConn(conIdx)
  result = await conn.query(pstmt,params)
  pool.returnConn(conIdx)

{.push warning[ObservableStores]: off.}
proc rawExec*(pool: AsyncPool, qs: string): Future[ResultSet[string]] {.
               async,#[ tags: [ReadDbEffect, WriteDbEffect,RootEffect]]#.} =
  let conIdx = await pool.getFreeConnIdx()
  let conn = await pool.getFreeConn(conIdx)
  result = await conn.rawExec(qs)
  pool.returnConn(conIdx)

proc rawQuery*(pool: AsyncPool, qs: string, onlyFirst:bool = false): Future[ResultSet[string]] {.
               async, #[ tags: [ReadDbEffect, WriteDbEffect,RootEffect]]#.} =
  let conIdx = await pool.getFreeConnIdx()
  let conn = await pool.getFreeConn(conIdx)
  result = await conn.rawQuery(qs,onlyFirst)
  pool.returnConn(conIdx)
{.pop.}

proc query*(pool: AsyncPool, pstmt: SqlPrepared, params: varargs[SqlParam, asParam]): Future[ResultSet[ResultValue]] {.
            asyncVarargs#[tags: [ReadDbEffect, WriteDbEffect]]#.} =
  let conIdx = await pool.getFreeConnIdx()
  let conn = await pool.getFreeConn(conIdx)
  result = await conn.query(pstmt, params)
  pool.returnConn(conIdx)

proc selectDatabase*(pool: AsyncPool, database: string): Future[ResponseOK] {.async.} =
  let conIdx = await pool.getFreeConnIdx()
  let conn = await pool.getFreeConn(conIdx)
  result = await conn.selectDatabase(database)
  pool.returnConn(conIdx)

proc exec*(pool: AsyncPool, qs: SqlQuery, args: varargs[string, `$`]): Future[ResultSet[string]] {.
           asyncVarargs,  #[tags: [ReadDbEffect]]#.} =
  let conIdx = await pool.getFreeConnIdx()
  let conn = await pool.getFreeConn(conIdx)
  result = await conn.exec(qs, args)
  pool.returnConn(conIdx)

proc query*(pool: AsyncPool, qs: SqlQuery, args: varargs[string, `$`],
            onlyFirst:static[bool] = false): Future[ResultSet[string]] {.asyncVarargs.} =
  let conIdx = await pool.getFreeConnIdx()
  let conn = await pool.getFreeConn(conIdx)
  var q = dbFormat(qs, args)
  result = await conn.rawQuery(q, onlyFirst)
  pool.returnConn(conIdx)

proc tryQuery*(pool: AsyncPool, qs: SqlQuery, args: varargs[string, `$`]): Future[bool] {.
               asyncVarargs, #[tags: [ReadDbEffect]]#.} =
  ## tries to execute the query and returns true if successful, false otherwise.
  let conIdx = await pool.getFreeConnIdx()
  let conn = await pool.getFreeConn(conIdx)
  result = await conn.tryQuery(qs, args)
  pool.returnConn(conIdx)

proc getRow*(pool: AsyncPool, qs: SqlQuery,
             args: varargs[string, `$`]): Future[Row] {.asyncVarargs,  #[tags: [ReadDbEffect]]#.} =
  ## Retrieves a single row. If the query doesn't return any rows, this proc
  ## will return a Row with empty strings for each column.
  let conIdx = await pool.getFreeConnIdx()
  let conn = await pool.getFreeConn(conIdx)
  result = await conn.getRow(qs, args)
  pool.returnConn(conIdx)

proc getAllRows*(pool: AsyncPool, qs: SqlQuery,
                 args: varargs[string, `$`]): Future[seq[Row]] {.asyncVarargs,  #[tags: [ReadDbEffect]]#.} =
  ## executes the query and returns the whole result dataset.
  let conIdx = await pool.getFreeConnIdx()
  let conn = await pool.getFreeConn(conIdx)
  result = await conn.getAllRows(qs, args)
  pool.returnConn(conIdx)

proc getValue*(pool: AsyncPool, qs: SqlQuery,
               args: varargs[string, `$`]): Future[string] {.asyncVarargs,  #[tags: [ReadDbEffect]]#.} =
  ## executes the query and returns the first column of the first row of the
  ## result dataset. Returns "" if the dataset contains no rows or the database
  ## value is NULL.
  let conIdx = await pool.getFreeConnIdx()
  let conn = await pool.getFreeConn(conIdx)
  result = await conn.getValue(qs, args)
  pool.returnConn(conIdx)

proc tryInsertId*(pool: AsyncPool, qs: SqlQuery,
                  args: varargs[string, `$`]): Future[int64] {.asyncVarargs, #[raises: [], tags: [WriteDbEffect]]#.} =
  ## executes the query (typically "INSERT") and returns the
  ## generated ID for the row or -1 in case of an error.
  let conIdx = await pool.getFreeConnIdx()
  let conn = await pool.getFreeConn(conIdx)
  result = await conn.tryInsertId(qs, args)
  pool.returnConn(conIdx)

proc insertId*(pool: AsyncPool, qs: SqlQuery,
               args: varargs[string, `$`]): Future[int64] {.asyncVarargs,  #[tags: [WriteDbEffect]]#.} =
  ## executes the query (typically "INSERT") and returns the
  ## generated ID for the row.
  let conIdx = await pool.getFreeConnIdx()
  let conn = await pool.getFreeConn(conIdx)
  result = await conn.insertId(qs, args)
  pool.returnConn(conIdx)

proc tryInsert*(pool: AsyncPool, qs: SqlQuery, pkName: string,
                args: varargs[string, `$`]): Future[int64] {.asyncVarargs, #[raises: [], tags: [WriteDbEffect]]#.} =
  ## same as tryInsertID
  let conIdx = await pool.getFreeConnIdx()
  let conn = await pool.getFreeConn(conIdx)
  result = await tryInsertID(conn, qs, args)
  pool.returnConn(conIdx)

proc insert*(pool: AsyncPool, qs: SqlQuery, pkName: string,
             args: varargs[string, `$`]): Future[int64]
            {.asyncVarargs,  #[tags: [WriteDbEffect]]#.} =
  ## same as insertId
  let conIdx = await pool.getFreeConnIdx()
  let conn = await pool.getFreeConn(conIdx)
  result = await conn.insert(qs, pkName, args)
  pool.returnConn(conIdx)
