
when defined(ChronosAsync):
  import chronos
else:
  import asyncdispatch
import macros
import urlly
import ../amysql 
import ../amysql/private/format
import ./async_varargs

type
  ## db pool
  AsyncPoolRef* = ref object
    conns: seq[Connection]
    busy: seq[bool]

proc newAsyncPool*(
    host,
    user,
    password,
    database: string,
    num: int
  ): Future[AsyncPoolRef] {.async.} =
  ## Create a new async pool of num connections.
  result =  new AsyncPoolRef
  var connIns:Connection
  for i in 0 ..< num:
    try:
      connIns = await amysql.open(host, user, password, database)
    except Exception as e:
      echo e.msg
    result.conns.add connIns
    result.busy.add false

proc newAsyncPool*(
    uriStr: string | urlly.Url,
    num: int
  ): Future[AsyncPoolRef] {.async.} =
  ## Create a new async pool of num connections.
  result = new AsyncPoolRef
  for i in 0 ..< num:
    let conn = await amysql.open(uriStr)
    result.conns.add conn
    result.busy.add false

proc getFreeConnIdx*(pool: AsyncPoolRef): Future[int] {.async.} =
  ## Wait for a free connection and return it.
  while true:
    for conIdx in 0 ..< pool.conns.len:
      if not pool.busy[conIdx]:
        pool.busy[conIdx] = true
        return conIdx
    await sleepAsync(0)

proc getFreeConn*(pool: AsyncPoolRef, conIdx: int):Connection =
  result = pool.conns[conIdx]

proc returnConn*(pool: AsyncPoolRef, conIdx: int) =
  ## Make the connection as free after using it and getting results.
  pool.busy[conIdx] = false

template withConn(pool: AsyncPoolRef,connIns, body) =
  let conIdx = await pool.getFreeConnIdx()
  var connIns  = pool.conns[conIdx]
  when ResetConnection:
    discard await connIns.reset()
  body
  pool.returnConn(conIdx)

proc close*(pool: AsyncPoolRef) {.async.} = 
  for conn in pool.conns:
    await conn.close()

proc query*(pool: AsyncPoolRef, pstmt: SqlPrepared, params: openarray[static[SqlParam]]): Future[void] {.async.}=
  pool.withConn(connIns):
    result = await connIns.query(pstmt,params)

{.push warning[ObservableStores]: off.}
proc rawExec*(pool: AsyncPoolRef, qs: string): Future[ResultSet[string]] {.
               async,#[ tags: [ReadDbEffect, WriteDbEffect,RootEffect]]#.} =
  pool.withConn(connIns):
    result = await connIns.rawExec(qs)

proc rawQuery*(pool: AsyncPoolRef, qs: string, onlyFirst:bool = false): Future[ResultSet[string]] {.
               async, #[ tags: [ReadDbEffect, WriteDbEffect,RootEffect]]#.} =
  pool.withConn(connIns):
    result = await connIns.rawQuery(qs,onlyFirst)
{.pop.}

proc query*(pool: AsyncPoolRef, pstmt: SqlPrepared, params: varargs[SqlParam, asParam]): Future[ResultSet[ResultValue]] {.
            asyncVarargs#[tags: [ReadDbEffect, WriteDbEffect]]#.} =
  pool.withConn(connIns):
    result = await connIns.query(pstmt, params)

proc selectDatabase*(pool: AsyncPoolRef, database: string): Future[ResponseOK] {.async.} =
  pool.withConn(connIns):
    result = await connIns.selectDatabase(database)

proc ping*(pool: AsyncPoolRef): Future[ResponseOK] {.async.} =
  pool.withConn(connIns):
    result = await connIns.ping()

proc exec*(pool: AsyncPoolRef, qs: SqlQuery, args: varargs[string, `$`]): Future[ResultSet[string]] {.
           asyncVarargs,  #[tags: [ReadDbEffect]]#.} =
  pool.withConn(connIns):
    result = await connIns.exec(qs, args)

proc query*(pool: AsyncPoolRef, qs: SqlQuery, args: varargs[string, `$`],
            onlyFirst:static[bool] = false): Future[ResultSet[string]] {.asyncVarargs.} =
  pool.withConn(connIns):
    var q = dbFormat(qs, args)
    result = await connIns.rawQuery(q, onlyFirst)

proc tryQuery*(pool: AsyncPoolRef, qs: SqlQuery, args: varargs[string, `$`]): Future[bool] {.
               asyncVarargs, #[tags: [ReadDbEffect]]#.} =
  ## tries to execute the query and returns true if successful, false otherwise.
  pool.withConn(connIns):
    result = await connIns.tryQuery(qs, args)

proc getRow*(pool: AsyncPoolRef, qs: SqlQuery,
             args: varargs[string, `$`]): Future[Row] {.asyncVarargs,  #[tags: [ReadDbEffect]]#.} =
  ## Retrieves a single row. If the query doesn't return any rows, this proc
  ## will return a Row with empty strings for each column.
  pool.withConn(connIns):
    result = await connIns.getRow(qs, args)

proc getAllRows*(pool: AsyncPoolRef, qs: SqlQuery,
                 args: varargs[string, `$`]): Future[seq[Row]] {.asyncVarargs,  #[tags: [ReadDbEffect]]#.} =
  ## executes the query and returns the whole result dataset.
  pool.withConn(connIns):
    result = await connIns.getAllRows(qs, args)

proc getValue*(pool: AsyncPoolRef, qs: SqlQuery,
               args: varargs[string, `$`]): Future[string] {.asyncVarargs,  #[tags: [ReadDbEffect]]#.} =
  ## executes the query and returns the first column of the first row of the
  ## result dataset. Returns "" if the dataset contains no rows or the database
  ## value is NULL.
  pool.withConn(connIns):
    result = await connIns.getValue(qs, args)

proc tryInsertId*(pool: AsyncPoolRef, qs: SqlQuery,
                  args: varargs[string, `$`]): Future[int64] {.asyncVarargs, #[raises: [], tags: [WriteDbEffect]]#.} =
  ## executes the query (typically "INSERT") and returns the
  ## generated ID for the row or -1 in case of an error.
  pool.withConn(connIns):
    result = await connIns.tryInsertId(qs, args)

proc insertId*(pool: AsyncPoolRef, qs: SqlQuery,
               args: varargs[string, `$`]): Future[int64] {.asyncVarargs,  #[tags: [WriteDbEffect]]#.} =
  ## executes the query (typically "INSERT") and returns the
  ## generated ID for the row.
  pool.withConn(connIns):
    result = await connIns.insertId(qs, args)

proc tryInsert*(pool: AsyncPoolRef, qs: SqlQuery, pkName: string,
                args: varargs[string, `$`]): Future[int64] {.asyncVarargs, #[raises: [], tags: [WriteDbEffect]]#.} =
  ## same as tryInsertID
  pool.withConn(connIns):
    result = await tryInsertID(connIns, qs, args)

proc insert*(pool: AsyncPoolRef, qs: SqlQuery, pkName: string,
             args: varargs[string, `$`]): Future[int64]
            {.asyncVarargs,  #[tags: [WriteDbEffect]]#.} =
  ## same as insertId
  pool.withConn(connIns):
    result = await connIns.insert(qs, pkName, args)
