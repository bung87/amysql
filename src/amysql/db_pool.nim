
{.experimental: "dotOperators".}

import ./conn
import locks
import times
import asyncdispatch
import ../amysql
import uri
import strutils
import macros
import strformat
import asyncnet
import sets,hashes
import times

type 
  DBPool* = ref DBPoolObj
  DBPoolObj = object
    freeConn: HashSet[DBConn]
    closed: bool
    maxPoolSize: int
    numOpen: int
    maxLifetime: Duration # maximum amount of time a connection may be reused
    maxIdleTime: Duration # maximum amount of time a connection may be idle before being closed
    openUri: Uri
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
    none,query,prepare,finalize,reset,exec,close
  DBStmt {.pure.} = object
    case kind:StmtKind
    of text:
      textVal:string
    of binary:
      binVal:SqlPrepared
    of command:
      discard
    
    case met:InterfaceKind
    of InterfaceKind.query:
      params:seq[SqlParam]
    of InterfaceKind.none,InterfaceKind.close:
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
    pool: DBPool
    createdAt: DateTime #time.Time
    lock: Lock  # guards following
    conn: ptr Connection
    needReset: bool # The connection session should be reset before use if true.
    closed: bool
    # guarded by db.mu
    inUse: bool
    returnedAt: DateTime # Time the connection was created or returned.
    reader: ptr Channel[DBStmt]
    writer: ptr Channel[DBResult] # same as pool's reader

proc newDBConn*(writer:ptr Channel[DBResult]): DBConn =
  new result
  result.createdAt = now()
  result.returnedAt = now()
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
  return self.createdAt + timeout < now()


type 
  Context = ref ContextObj
  ContextObj = object
    dbConn: DBConn
    openUri: Uri

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
        of InterfaceKind.query:
          let r = await ctx.dbConn.conn[].rawQuery(st.textVal )
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
        else:
          discard

proc worker(ctx:Context) {.thread.} =
  asyncCheck workerProcess(ctx)
  runForever()

proc handleParams(query: string,minPoolSize,maxPoolSize:var int):string =
  var key, val: string
  var pos = 0
  for item in split(query,"&"):
    (key, val) = item.split("=")
    case key
    of "minPoolSize":
      minPoolSize = parseInt(val)
    of "maxPoolSize":
      maxPoolSize = parseInt(val)
    else:
      result.add key & '=' & val
      inc pos

proc hash(x: DBConn): Hash = 
  var h: Hash = 0
  h = h !& hash($x.createdAt)
  result = !$h

proc newDBPool*(uriStr: string | Uri): Future[DBPool] {.async.} = 
  ## min pool size
  ## max pool size
  ## max idle timeout exceed then close,affected freeConns,numOpen
  ## max open timeout
  ## max lifetime exceed then close,affected freeConns,numOpen
  new result
  var uri:Uri = when uriStr is string: parseUri(uriStr) else: uriStr
  var minPoolSize,maxPoolSize:int
  uri.query = handleParams(uri.query,minPoolSize,maxPoolSize)
  debugEcho fmt"minPoolSize:{$minPoolSize},maxPoolSize:{$maxPoolSize}"
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

proc close*(self:DBPool): Future[void] {.async.}=
  for dbConn in  self.freeConn:
    discard dbConn.reader[].trySend(DBStmt(met:InterfaceKind.close,kind:StmtKind.command))
    discard dbConn.writer[].tryRecv()
    if dbConn.reader[].peek() != -1:
      dbConn.reader[].close()
  if self.reader[].peek() != -1:
    self.reader[].close()
  deallocShared(self.reader)
  self.closed = true

proc fetchConn*(self: DBPool): Future[DBConn] {.async.} = 
  ## conn returns a newly-opened or new DBconn.
  if self.closed:
    raise newException(IOError,"Connection closed.")
    # return nil, errDBClosed
  debugEcho fmt"self.freeConn:{$self.freeConn.len},self.numOpen:{$self.numOpen},self.maxPoolSize:{$self.maxPoolSize}"
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
      debugEcho fmt"expired: {$lifetime}"
      await result.close
      # return nil, driver.ErrBadConn

    ## Reset the session if required.
    # let err = conn.resetSession(ctx)
    # if err == driver.ErrBadConn:
    #   await conn.close()
    #   return nil, driver.ErrBadConn
    return result

  # no free conn, open new
  debugEcho "no free conn, open new"
  result = newDBConn(self.reader)
  result.pool = self
  
  self.freeConn.incl result
  var ctx = Context(dbConn: result,openUri: self.openUri)
  var thread: Thread[Context]
  createThread(thread, worker, ctx)
  inc self.numOpen
  # wait for established
  discard self.reader[].recv()

proc rawQuery*(self: DBPool, query: string, onlyFirst:static[bool] = false): Future[ResultSet[string]] {.
               async.} =
  let conn = await self.fetchConn()
  conn.reader[].send(DBStmt(met:InterfaceKind.query,kind:StmtKind.text,textVal:query))
  let msg = conn.writer[].recv()
  self.freeConn.incl conn
  return msg.textVal

proc query(self: DBPool, pstmt: DBSqlPrepared, params: seq[SqlParam]): Future[ResultSet[ResultValue]] {.async.} =
  let conn = pstmt.dbConn
  conn.reader[].send(DBStmt(met:InterfaceKind.query,kind:StmtKind.binary,binVal:pstmt.pstmt,params:params))
  let msg = conn.writer[].recv()
  self.freeConn.incl conn
  return msg.binVal

proc query*(self: DBPool, pstmt: DBSqlPrepared, params: varargs[SqlParam, asParam]): Future[ResultSet[ResultValue]] =
  let p:seq[SqlParam] = @params
  self.query(pstmt,p)

proc rawExec*(self: DBPool, query: string): Future[ResultSet[string]] {.
               async.} =
  let conn = await self.fetchConn()
  conn.reader[].send(DBStmt(met:InterfaceKind.exec,kind:StmtKind.text,textVal:query))
  let msg = conn.writer[].recv()
  self.freeConn.incl conn
  return msg.textVal

proc prepare*(self: DBPool, query: string): Future[DBSqlPrepared] {.async.} =
  let conn = await self.fetchConn()
  conn.reader[].send(DBStmt(met:InterfaceKind.prepare,kind:StmtKind.binary,q:query))
  let msg = conn.writer[].recv()
  return DBSqlPrepared(pstmt:msg.pstmt,dbConn:conn)

proc finalize*(self: DBPool, pstmt: DBSqlPrepared): Future[void] {.async.} =
  pstmt.dbConn.reader[].send(DBStmt(met:InterfaceKind.finalize,kind:StmtKind.binary,pstmt:pstmt.pstmt))
  discard pstmt.dbConn.writer[].recv()
  self.freeConn.incl pstmt.dbConn

proc reset*(self: DBPool, pstmt: DBSqlPrepared): Future[void] {.async.} =
  pstmt.dbConn.reader[].send(DBStmt(met:InterfaceKind.reset,kind:StmtKind.binary,pstmt:pstmt.pstmt))
  discard pstmt.dbConn.writer[].recv()
