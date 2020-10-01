
import asyncdispatch
import macros
import uri
import ../amysql
import amysql/private/format

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

proc query*(pool: AsyncPool, pstmt: SqlPrepared, params: openarray[static[SqlParam]]): Future[void] {.async.}=
  let conIdx = await pool.getFreeConnIdx()
  let conn = pool.conns[conIdx]
  result = await conn.query(query,params)
  pool.returnConn(conIdx)

{.push warning[ObservableStores]: off.}
proc rawExec*(pool: AsyncPool, query: string): Future[ResultSet[string]] {.
               async,#[ tags: [ReadDbEffect, WriteDbEffect,RootEffect]]#.} =
  let conIdx = await pool.getFreeConnIdx()
  let conn = pool.conns[conIdx]
  result = await conn.rawExec(query)
  pool.returnConn(conIdx)

proc rawQuery*(pool: AsyncPool, query: string, onlyFirst:bool = false): Future[ResultSet[string]] {.
               async, #[ tags: [ReadDbEffect, WriteDbEffect,RootEffect]]#.} =
  let conIdx = await pool.getFreeConnIdx()
  let conn = pool.conns[conIdx]
  result = await conn.rawQuery(query,onlyFirst)
  pool.returnConn(conIdx)
{.pop.}

proc query*(pool: AsyncPool, pstmt: SqlPrepared, params: varargs[SqlParam, asParam]): Future[ResultSet[ResultValue]] {.
            async#[tags: [ReadDbEffect, WriteDbEffect]]#.} =
  let conIdx = await pool.getFreeConnIdx()
  let conn = pool.conns[conIdx]
  var pkt = conn.formatBoundParams(pstmt, params)
  var sent = conn.sendPacket(pkt, resetSeqId=true)
  result = await performPreparedQuery(conn, pstmt, sent)
  pool.returnConn(conIdx)

proc selectDatabase*(pool: AsyncPool, database: string): Future[ResponseOK] {.async.} =
  let conIdx = await pool.getFreeConnIdx()
  let conn = pool.conns[conIdx]
  result = await conn.selectDatabase(database)
  pool.returnConn(conIdx)

proc exec*(pool: AsyncPool, query: SqlQuery, args: varargs[string, `$`]): Future[ResultSet[string]] {.
            async,  #[tags: [ReadDbEffect]]#.} =
  let conIdx = await pool.getFreeConnIdx()
  let conn = pool.conns[conIdx]
  var q = dbFormat(query, args)
  result = await conn.rawExec(q)
  pool.returnConn(conIdx)

proc query*(pool: AsyncPool, query: SqlQuery, args: varargs[string, `$`], onlyFirst:static[bool] = false): Future[ResultSet[string]] {.
            async,  #[tags: [ReadDbEffect]]#.} =
  let conIdx = await pool.getFreeConnIdx()
  let conn = pool.conns[conIdx]
  var q = dbFormat(query, args)
  result = await conn.rawQuery(q, onlyFirst)
  pool.returnConn(conIdx)

proc tryQuery*(pool: AsyncPool, query: SqlQuery, args: varargs[string, `$`]): Future[bool] {.
               async, #[tags: [ReadDbEffect]]#.} =
  ## tries to execute the query and returns true if successful, false otherwise.
  let conIdx = await pool.getFreeConnIdx()
  let conn = pool.conns[conIdx]
  result = true
  try:
    discard await conn.exec(query, args)
  except:
    result = false
  pool.returnConn(conIdx)

proc getRow*(pool: AsyncPool, query: SqlQuery,
             args: varargs[string, `$`]): Future[Row] {.async,  #[tags: [ReadDbEffect]]#.} =
  ## Retrieves a single row. If the query doesn't return any rows, this proc
  ## will return a Row with empty strings for each column.
  let conIdx = await pool.getFreeConnIdx()
  let conn = pool.conns[conIdx]
  let resultSet = await conn.query(query, args, onlyFirst = true)
  if resultSet.rows.len == 0:
    let cols = resultSet.columns.len
    result = newSeq[string](cols)
  else:
    result = resultSet.rows[0]
  pool.returnConn(conIdx)

proc getAllRows*(pool: AsyncPool, query: SqlQuery,
                 args: varargs[string, `$`]): Future[seq[Row]] {.async,  #[tags: [ReadDbEffect]]#.} =
  ## executes the query and returns the whole result dataset.
  let conIdx = await pool.getFreeConnIdx()
  let conn = pool.conns[conIdx]
  let resultSet = await conn.query(query, args)
  result = resultSet.rows
  pool.returnConn(conIdx)

proc getValue*(pool: AsyncPool, query: SqlQuery,
               args: varargs[string, `$`]): Future[string] {.async,  #[tags: [ReadDbEffect]]#.} =
  ## executes the query and returns the first column of the first row of the
  ## result dataset. Returns "" if the dataset contains no rows or the database
  ## value is NULL.
  let conIdx = await pool.getFreeConnIdx()
  let conn = pool.conns[conIdx]
  let row = await getRow(conn, query, args)
  result = row[0]
  pool.returnConn(conIdx)

proc tryInsertId*(pool: AsyncPool, query: SqlQuery,
                  args: varargs[string, `$`]): Future[int64] {.async, #[raises: [], tags: [WriteDbEffect]]#.} =
  ## executes the query (typically "INSERT") and returns the
  ## generated ID for the row or -1 in case of an error.
  let conIdx = await pool.getFreeConnIdx()
  let conn = pool.conns[conIdx]
  var resultSet:ResultSet[string]
  try:
    resultSet = await conn.exec(query, args)
  except:
    result = -1'i64
    pool.returnConn(conIdx)
    return result
  result = resultSet.status.last_insert_id.int64
  pool.returnConn(conIdx)

proc insertId*(pool: AsyncPool, query: SqlQuery,
               args: varargs[string, `$`]): Future[int64] {.async,  #[tags: [WriteDbEffect]]#.} =
  ## executes the query (typically "INSERT") and returns the
  ## generated ID for the row.
  let conIdx = await pool.getFreeConnIdx()
  let conn = pool.conns[conIdx]
  let resultSet = await conn.exec(query, args)
  result = resultSet.status.last_insert_id.int64
  pool.returnConn(conIdx)

proc tryInsert*(pool: AsyncPool, query: SqlQuery, pkName: string,
                args: varargs[string, `$`]): Future[int64] {.async, #[raises: [], tags: [WriteDbEffect]]#.} =
  ## same as tryInsertID
  let conIdx = await pool.getFreeConnIdx()
  let conn = pool.conns[conIdx]
  result = await tryInsertID(conn, query, args)
  pool.returnConn(conIdx)

proc insert*(pool: AsyncPool, query: SqlQuery, pkName: string,
             args: varargs[string, `$`]): Future[int64]
            {.async,  #[tags: [WriteDbEffect]]#.} =
  ## same as insertId
  let conIdx = await pool.getFreeConnIdx()
  let conn = pool.conns[conIdx]
  let resultSet = await conn.exec(query, args)
  result = resultSet.status.last_insert_id.int64
  pool.returnConn(conIdx)

