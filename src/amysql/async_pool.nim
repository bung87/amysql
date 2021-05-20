
when defined(ChronosAsync):
  import chronos
else:
  import asyncdispatch
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
    await sleepAsync(0)

proc getFreeConn*(pool: AsyncPool, conIdx: int):Connection =
  result = pool.conns[conIdx]

proc returnConn*(pool: AsyncPool, conIdx: int) =
  ## Make the connection as free after using it and getting results.
  pool.busy[conIdx] = false

template withConn(pool: AsyncPool, conn, body) =
  let conIdx = await pool.getFreeConnIdx()
  var conn = pool.conns[conIdx]
  when ResetConnection:
    discard await conn.reset()
  body
  pool.returnConn(conIdx)

proc close*(pool: AsyncPool) {.async.} = 
  for conn in pool.conns:
    await conn.close()

proc query*(pool: AsyncPool, pstmt: SqlPrepared, params: openarray[static[SqlParam]]): Future[void] {.async.}=
  pool.withConn(conn):
    result = await conn.query(pstmt,params)

{.push warning[ObservableStores]: off.}
proc rawExec*(pool: AsyncPool, qs: string): Future[ResultSet[string]] {.
               async,#[ tags: [ReadDbEffect, WriteDbEffect,RootEffect]]#.} =
  pool.withConn(conn):
    result = await conn.rawExec(qs)

proc rawQuery*(pool: AsyncPool, qs: string, onlyFirst:bool = false): Future[ResultSet[string]] {.
               async, #[ tags: [ReadDbEffect, WriteDbEffect,RootEffect]]#.} =
  pool.withConn(conn):
    result = await conn.rawQuery(qs,onlyFirst)
{.pop.}

proc query*(pool: AsyncPool, pstmt: SqlPrepared, params: varargs[SqlParam, asParam]): Future[ResultSet[ResultValue]] {.
            asyncVarargs#[tags: [ReadDbEffect, WriteDbEffect]]#.} =
  pool.withConn(conn):
    result = await conn.query(pstmt, params)

proc selectDatabase*(pool: AsyncPool, database: string): Future[ResponseOK] {.async.} =
  pool.withConn(conn):
    result = await conn.selectDatabase(database)

proc exec*(pool: AsyncPool, qs: SqlQuery, args: varargs[string, `$`]): Future[ResultSet[string]] {.
           asyncVarargs,  #[tags: [ReadDbEffect]]#.} =
  pool.withConn(conn):
    result = await conn.exec(qs, args)

proc query*(pool: AsyncPool, qs: SqlQuery, args: varargs[string, `$`],
            onlyFirst:static[bool] = false): Future[ResultSet[string]] {.asyncVarargs.} =
  pool.withConn(conn):
    var q = dbFormat(qs, args)
    result = await conn.rawQuery(q, onlyFirst)

proc tryQuery*(pool: AsyncPool, qs: SqlQuery, args: varargs[string, `$`]): Future[bool] {.
               asyncVarargs, #[tags: [ReadDbEffect]]#.} =
  ## tries to execute the query and returns true if successful, false otherwise.
  pool.withConn(conn):
    result = await conn.tryQuery(qs, args)

proc getRow*(pool: AsyncPool, qs: SqlQuery,
             args: varargs[string, `$`]): Future[Row] {.asyncVarargs,  #[tags: [ReadDbEffect]]#.} =
  ## Retrieves a single row. If the query doesn't return any rows, this proc
  ## will return a Row with empty strings for each column.
  pool.withConn(conn):
    result = await conn.getRow(qs, args)

proc getAllRows*(pool: AsyncPool, qs: SqlQuery,
                 args: varargs[string, `$`]): Future[seq[Row]] {.asyncVarargs,  #[tags: [ReadDbEffect]]#.} =
  ## executes the query and returns the whole result dataset.
  pool.withConn(conn):
    result = await conn.getAllRows(qs, args)

proc getValue*(pool: AsyncPool, qs: SqlQuery,
               args: varargs[string, `$`]): Future[string] {.asyncVarargs,  #[tags: [ReadDbEffect]]#.} =
  ## executes the query and returns the first column of the first row of the
  ## result dataset. Returns "" if the dataset contains no rows or the database
  ## value is NULL.
  pool.withConn(conn):
    result = await conn.getValue(qs, args)

proc tryInsertId*(pool: AsyncPool, qs: SqlQuery,
                  args: varargs[string, `$`]): Future[int64] {.asyncVarargs, #[raises: [], tags: [WriteDbEffect]]#.} =
  ## executes the query (typically "INSERT") and returns the
  ## generated ID for the row or -1 in case of an error.
  pool.withConn(conn):
    result = await conn.tryInsertId(qs, args)

proc insertId*(pool: AsyncPool, qs: SqlQuery,
               args: varargs[string, `$`]): Future[int64] {.asyncVarargs,  #[tags: [WriteDbEffect]]#.} =
  ## executes the query (typically "INSERT") and returns the
  ## generated ID for the row.
  pool.withConn(conn):
    result = await conn.insertId(qs, args)

proc tryInsert*(pool: AsyncPool, qs: SqlQuery, pkName: string,
                args: varargs[string, `$`]): Future[int64] {.asyncVarargs, #[raises: [], tags: [WriteDbEffect]]#.} =
  ## same as tryInsertID
  pool.withConn(conn):
    result = await tryInsertID(conn, qs, args)

proc insert*(pool: AsyncPool, qs: SqlQuery, pkName: string,
             args: varargs[string, `$`]): Future[int64]
            {.asyncVarargs,  #[tags: [WriteDbEffect]]#.} =
  ## same as insertId
  pool.withConn(conn):
    result = await conn.insert(qs, pkName, args)
