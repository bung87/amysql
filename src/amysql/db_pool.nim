
##
## This module implements a threaded connection pool
##
## This is currently very experimental.
##
## Copyright (c) 2020 Bung

import ./conn
import locks
when defined(ChronosAsync):
  import chronos/[asyncloop, asyncsync, handles, transport, timer]
  import times except milliseconds,Duration,toParts,DurationZero,initDuration
  const DurationZero = default(Duration)
else:
  import asyncdispatch,asyncnet,times
import ../amysql
import urlly
import strutils
import macros
import strformat
import sets,hashes
import cpuinfo
import db_common
import ./private/format
import logging

when defined(release):  setLogFilter(lvlInfo)

type 
  DBPoolRef* = ref DBPoolObj
  DBPoolObj = object
    freeConn: HashSet[DBConn]
    closed: bool
    maxPoolSize: int
    numOpen: int
    maxLifetime: Duration # maximum amount of time a connection may be reused
    maxIdleTime: Duration # maximum amount of time a connection may be idle before being closed
    openUri: urlly.Url
    reader:ptr Channel[DBResult]
   
  DBConn* = ref DBConnObj
  DBSqlPrepared* = object
    pstmt:SqlPrepared
    dbConn:DBConn
  StmtKind = enum
    text,
    binary,
    command
  InterfaceKind {.pure.} = enum
    none,query,rawQuery,prepare,finalize,reset,exec,close,resetConnection
  DBStmt {.pure.} = object
    case kind:StmtKind
    of text:
      textVal:string
    of binary:
      binVal:SqlPrepared
    of command:
      discard
    
    case met:InterfaceKind
    of InterfaceKind.rawQuery:
      onlyFirst:bool
    of InterfaceKind.query:
      params:seq[SqlParam]
    of InterfaceKind.none,InterfaceKind.close,InterfaceKind.resetConnection:
      discard
    of InterfaceKind.prepare,InterfaceKind.exec:
      q:string
    of InterfaceKind.finalize,InterfaceKind.reset:
      pstmt:SqlPrepared
  DBResultKind = enum
    resultsetText,resultsetBinary,pstmt,none
  DBResult {.pure.} = object
    case kind:DBResultKind
    of resultsetText:
      textVal:ResultSet[string]
    of resultsetBinary:
      binVal:ResultSet[ResultValue]
    of pstmt:
      pstmt:SqlPrepared
    of none:
      discard
    
  DBConnObj = object
    pool: DBPoolRef
    when not defined(ChronosAsync):
      createdAt: DateTime #time.Time
      returnedAt: DateTime # Time the connection was created or returned.
    else:
      createdAt: Moment
      returnedAt: Moment
    lock: Lock  # guards following
    conn: ptr Connection
    needReset: bool # The connection session should be reset before use if true.
    closed: bool
    # guarded by db.mu
    inUse: bool
   
    reader: ptr Channel[DBStmt]
    writer: ptr Channel[DBResult] # same as pool's reader

proc newDBConn*(writer:ptr Channel[DBResult]): DBConn =
  new result
  when not defined(ChronosAsync):
    result.createdAt = now()
    result.returnedAt = now()
  else:
    result.createdAt = Moment.now()
    result.returnedAt = Moment.now()
  result.reader = cast[ptr Channel[DBStmt]](
    allocShared0(sizeof(Channel[DBStmt]))
  )
  result.writer = writer
  result.reader[].open()

proc close*(self: DBConn) {.async.} =
  self.reader[].close
  deallocShared(self.reader)
  await self.conn[].close

proc expired*(self: DBConn, timeout:Duration): bool = 
  if timeout <= DurationZero:
    return false
  when not defined(ChronosAsync):
    let n = now()
  else:
    let n = Moment.now()
  return self.createdAt + timeout < n


type 
  Context = ref ContextObj
  ContextObj = object
    dbConn: DBConn
    openUri: urlly.Url

proc workerProcess(ctx: sink Context): Future[void] {.async.} = 
  var conn = await open(ctx.openUri)
  ctx.dbConn.conn = conn.addr
  let p = DBResult(kind:DBResultKind.none)
  ctx.dbConn.writer[].send(p)
  while true:
    let tried = ctx.dbConn.reader[].tryRecv()
    if tried.dataAvailable:
      let st = tried.msg
      case st.kind
      of StmtKind.text: 
        case st.met:
        of InterfaceKind.rawQuery:
          let r = await ctx.dbConn.conn[].rawQuery(st.textVal,onlyFirst = st.onlyFirst )
          let p = DBResult(kind:DBResultKind.resultsetText,textVal:r)
          discard ctx.dbConn.writer[].trySend(p)
        of InterfaceKind.exec:
          let r = await ctx.dbConn.conn[].rawExec(st.q)
          let p = DBResult(kind:DBResultKind.resultsetText,textVal:r)
          discard ctx.dbConn.writer[].trySend(p)
        else:
          discard
      of StmtKind.binary: 
        case st.met:
        of InterfaceKind.none:
          discard
        of InterfaceKind.query:
          let r = await ctx.dbConn.conn[].query(st.binVal,st.params)
          let p = DBResult(kind:DBResultKind.resultsetBinary,binVal:r)
          discard ctx.dbConn.writer[].trySend(p)
        of InterfaceKind.prepare:
          let r = await ctx.dbConn.conn[].prepare(st.q)
          let p = DBResult(kind:DBResultKind.pstmt,pstmt:r)
          discard ctx.dbConn.writer[].trySend(p)
        of InterfaceKind.finalize:
          await ctx.dbConn.conn[].finalize(st.pstmt)
          let p = DBResult(kind:DBResultKind.none)
          discard ctx.dbConn.writer[].trySend(p)
        of InterfaceKind.reset:
          await ctx.dbConn.conn[].reset(st.pstmt)
          let p = DBResult(kind:DBResultKind.none)
          discard ctx.dbConn.writer[].trySend(p)
        else:
          discard
      of StmtKind.command:
        case st.met:
        of InterfaceKind.close:
          await ctx.dbConn.conn[].close()
          let p = DBResult(kind:DBResultKind.none)
          discard ctx.dbConn.writer[].trySend(p)
          break
        of InterfaceKind.resetConnection:
          discard await ctx.dbConn.conn[].reset()
          let p = DBResult(kind:DBResultKind.none)
          discard ctx.dbConn.writer[].trySend(p)
        else:
          discard

proc worker(ctx:Context) {.thread.} =
  asyncCheck workerProcess(ctx)
  runForever()

proc handleParams(query: seq[(string,string)],minPoolSize,maxPoolSize:var int):seq[(string,string)] {.inline.} =
  var key, val: string
  var pos = 0
  for (key, val) in query:
    case key
    of "minPoolSize":
      minPoolSize = parseInt(val)
    of "maxPoolSize":
      maxPoolSize = parseInt(val)
    else:
      result.add (key ,val)
      inc pos

proc hash(x: DBConn): Hash = 
  var h: Hash = 0
  h = h !& hash($x.createdAt)
  result = !$h

proc newDBPool*(uriStr: string | Url): Future[DBPoolRef] {.async.} = 
  ## min pool size
  ## max pool size
  ## max idle timeout exceed then close,affected freeConns,numOpen
  ## max open timeout
  ## max lifetime exceed then close,affected freeConns,numOpen
  new result
  var uri:Url = when uriStr is string: parseUrl(uriStr) else: uriStr
  var minPoolSize = countProcessors() * 2 + 1
  var maxPoolSize:int
  uri.query = handleParams(uri.query,minPoolSize,maxPoolSize)
  debug fmt"minPoolSize:{$minPoolSize},maxPoolSize:{$maxPoolSize}"
  result.freeConn.init(minPoolSize)
  result.maxPoolSize = maxPoolSize
  result.openUri = uri
  result.reader = cast[ptr Channel[DBResult]](
    allocShared0(sizeof(Channel[DBResult]))
  )
  result.reader[].open()
  for i in 0 ..< minPoolSize:
    var dbConn = newDBConn( result.reader)
    dbConn.pool = result
    result.freeConn.incl dbConn
    var ctx = Context(dbConn: dbConn, openUri: result.openUri)
    var thread: Thread[Context]
    createThread(thread, worker, ctx)
    inc result.numOpen
    # wait for established
    discard result.reader[].recv()

proc close*(self:DBPoolRef): Future[void] {.async.}=
  for dbConn in  self.freeConn:
    discard dbConn.reader[].trySend(DBStmt(met:InterfaceKind.close,kind:StmtKind.command))
    discard dbConn.writer[].tryRecv()
    if dbConn.reader[].peek() != -1:
      dbConn.reader[].close()
  if self.reader[].peek() != -1:
    self.reader[].close()
  deallocShared(self.reader)
  self.closed = true

proc fetchConn*(self: DBPoolRef): Future[DBConn] {.async.} = 
  ## conn returns a newly-opened or new DBconn.
  if self.closed:
    raise newException(IOError,"Connection closed.")
    # return nil, errDBClosed
  ## Out of free connections or we were asked not to use one. If we're not
  ## allowed to open any more connections, make a request and wait.
  while len(self.freeConn) == 0 and self.numOpen >= self.maxPoolSize:
    await sleepAsync(1)
  
  let lifetime = self.maxLifetime

  ## Prefer a free connection, if possible.
  let numFree = len(self.freeConn)
  if numFree > 0:
    result = self.freeConn.pop
    result.inUse = true
    if result.expired(lifetime):
      debug fmt"expired: {$lifetime}"
      await result.close
      # return nil, driver.ErrBadConn

    ## Reset the session if required.
    when ResetConnection:
      result.reader[].send(DBStmt(met:InterfaceKind.resetConnection,kind:StmtKind.command))
      discard result.writer[].recv()
    return result

  # no free conn, open new
  debug "no free conn, open new"
  result = newDBConn(self.reader)
  result.pool = self
  
  self.freeConn.incl result
  var ctx = Context(dbConn: result,openUri: self.openUri)
  var thread: Thread[Context]
  createThread(thread, worker, ctx)
  inc self.numOpen
  # wait for established
  discard self.reader[].recv()

proc rawQuery*(self: DBPoolRef, query: string, onlyFirst:static[bool] = false): Future[ResultSet[string]] {.
               async.} =
  let conn = await self.fetchConn()
  conn.reader[].send(DBStmt(met:InterfaceKind.rawQuery,kind:StmtKind.text,textVal:query,onlyFirst:onlyFirst))
  let msg = conn.writer[].recv()
  self.freeConn.incl conn
  return msg.textVal

proc query(self: DBPoolRef, pstmt: DBSqlPrepared, params: seq[SqlParam]): Future[ResultSet[ResultValue]] {.async.} =
  let conn = pstmt.dbConn
  conn.reader[].send(DBStmt(met:InterfaceKind.query,kind:StmtKind.binary,binVal:pstmt.pstmt,params:params))
  let msg = conn.writer[].recv()
  self.freeConn.incl conn
  return msg.binVal

proc query*(self: DBPoolRef, pstmt: DBSqlPrepared, params: varargs[SqlParam, asParam]): Future[ResultSet[ResultValue]] =
  result = self.query(pstmt, @params)

proc rawExec*(self: DBPoolRef, query: string): Future[ResultSet[string]] {.
               async.} =
  let conn = await self.fetchConn()
  conn.reader[].send(DBStmt(met:InterfaceKind.exec,kind:StmtKind.text,textVal:query))
  let msg = conn.writer[].recv()
  self.freeConn.incl conn
  return msg.textVal

proc prepare*(self: DBPoolRef, query: string): Future[DBSqlPrepared] {.async.} =
  let conn = await self.fetchConn()
  conn.reader[].send(DBStmt(met:InterfaceKind.prepare,kind:StmtKind.binary,q:query))
  let msg = conn.writer[].recv()
  return DBSqlPrepared(pstmt:msg.pstmt,dbConn:conn)

proc finalize*(self: DBPoolRef, pstmt: DBSqlPrepared): Future[void] {.async.} =
  pstmt.dbConn.reader[].send(DBStmt(met:InterfaceKind.finalize,kind:StmtKind.binary,pstmt:pstmt.pstmt))
  discard pstmt.dbConn.writer[].recv()
  self.freeConn.incl pstmt.dbConn

proc reset*(self: DBPoolRef, pstmt: DBSqlPrepared): Future[void] {.async.} =
  pstmt.dbConn.reader[].send(DBStmt(met:InterfaceKind.reset,kind:StmtKind.binary,pstmt:pstmt.pstmt))
  discard pstmt.dbConn.writer[].recv()

proc exec(conn: DBPoolRef, query: SqlQuery, args: seq[string]): Future[ResultSet[string]] {.
            async, #[tags: [ReadDbEffect]]#.} =
  var q = dbFormat(query, args)
  result = await conn.rawExec(q)

proc exec*(conn: DBPoolRef, query: SqlQuery, args: varargs[string, `$`]): Future[ResultSet[string]] =
  result = conn.exec(query, @args)

proc query(conn: DBPoolRef, query: SqlQuery, args: seq[string], onlyFirst:static[bool] = false): Future[ResultSet[string]] {.
            async, #[tags: [ReadDbEffect]]#.} =
  var q = dbFormat(query, args)
  result = await conn.rawQuery(q, onlyFirst)

proc query*(conn: DBPoolRef, query: SqlQuery, args: varargs[string, `$`], onlyFirst:static[bool] = false): Future[ResultSet[string]] =
  result = conn.query(query, @args, onlyFirst)

proc tryQuery(conn: DBPoolRef, query: SqlQuery, args: seq[string]): Future[bool] {.
               async, #[tags: [ReadDbEffect]]#.} =
  ## tries to execute the query and returns true if successful, false otherwise.
  result = true
  try:
    discard await conn.exec(query, args)
  except:
    result = false
  return result

proc tryQuery*(conn: DBPoolRef, query: SqlQuery, args: varargs[string, `$`]): Future[bool] =
  ## tries to execute the query and returns true if successful, false otherwise.
  result = conn.tryQuery(query, @args)

proc getRow(conn: DBPoolRef, query: SqlQuery,
             args: seq[string]): Future[Row] {.async, #[tags: [ReadDbEffect]]#.} =
  ## Retrieves a single row. If the query doesn't return any rows, this proc
  ## will return a Row with empty strings for each column.
  let resultSet = await conn.query(query, args, onlyFirst = true)
  if resultSet.rows.len == 0:
    let cols = resultSet.columns.len
    result = newSeq[string](cols)
  else:
    result = resultSet.rows[0]

proc getRow*(conn: DBPoolRef, query: SqlQuery,
             args: varargs[string, `$`]): Future[Row] =
  ## Retrieves a single row. If the query doesn't return any rows, this proc
  ## will return a Row with empty strings for each column.
  result = conn.getRow(query, @args)

proc getAllRows(conn: DBPoolRef, query: SqlQuery,
                 args: seq[string]): Future[seq[Row]] {.async, #[tags: [ReadDbEffect]]#.} =
  ## executes the query and returns the whole result dataset.
  let resultSet = await conn.query(query, args)
  result = resultSet.rows

proc getAllRows*(conn: DBPoolRef, query: SqlQuery,
                 args: varargs[string, `$`]): Future[seq[Row]] =
  ## executes the query and returns the whole result dataset.
  result = conn.getAllRows(query, @args)

proc getValue(conn: DBPoolRef, query: SqlQuery,
               args: seq[string]): Future[string] {.async, #[tags: [ReadDbEffect]]#.} =
  ## executes the query and returns the first column of the first row of the
  ## result dataset. Returns "" if the dataset contains no rows or the database
  ## value is NULL.
  let row = await getRow(conn, query, args)
  result = row[0]

proc getValue*(conn: DBPoolRef, query: SqlQuery,
               args: varargs[string, `$`]): Future[string] =
  ## executes the query and returns the first column of the first row of the
  ## result dataset. Returns "" if the dataset contains no rows or the database
  ## value is NULL.
  result = conn.getValue(query, @args)

proc tryInsertId(conn: DBPoolRef, query: SqlQuery,
                  args: seq[string]): Future[int64] {.async, #[raises: [], tags: [WriteDbEffect]]#.} =
  ## executes the query (typically "INSERT") and returns the
  ## generated ID for the row or -1 in case of an error.
  var resultSet:ResultSet[string]
  try:
    resultSet = await conn.exec(query, args)
  except:
    result = -1'i64
    return result
  result = resultSet.status.lastInsertId.int64

proc tryInsertId*(conn: DBPoolRef, query: SqlQuery,
                  args: varargs[string, `$`]): Future[int64] =
  ## executes the query (typically "INSERT") and returns the
  ## generated ID for the row or -1 in case of an error.
  result = conn.tryInsertId(query, @args)

proc insertId(conn: DBPoolRef, query: SqlQuery,
               args: seq[string]): Future[int64] {.async, #[tags: [WriteDbEffect]]#.} =
  ## executes the query (typically "INSERT") and returns the
  ## generated ID for the row.
  let resultSet = await conn.exec(query, args)
  result = resultSet.status.lastInsertId.int64

proc insertId*(conn: DBPoolRef, query: SqlQuery,
               args: varargs[string, `$`]): Future[int64] =
  ## executes the query (typically "INSERT") and returns the
  ## generated ID for the row.
  result = conn.insertId(query, @args)

proc tryInsert(conn: DBPoolRef, query: SqlQuery, pkName: string,
                args: seq[string]): Future[int64] {.async,#[raises: [], tags: [WriteDbEffect]]#.} =
  ## same as tryInsertID
  result = await tryInsertID(conn, query, args)

proc tryInsert*(conn: DBPoolRef, query: SqlQuery, pkName: string,
                args: varargs[string, `$`]): Future[int64] =
  ## same as tryInsertID
  result = conn.tryInsert(query, pkName, @args)

proc insert(conn: DBPoolRef, query: SqlQuery, pkName: string,
             args: seq[string]): Future[int64]
            {.async, #[tags: [WriteDbEffect]]#.} =
  ## same as insertId
  let resultSet = await conn.exec(query, args)
  result = resultSet.status.lastInsertId.int64

proc insert*(conn: DBPoolRef, query: SqlQuery, pkName: string,
             args: varargs[string, `$`]): Future[int64] =
  ## same as insertId
  result = conn.insert(query, pkName, @args)