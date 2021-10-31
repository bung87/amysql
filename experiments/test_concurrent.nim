import asyncdispatch,httpclient
import locks
var L: Lock

let threadsNum = 512

when isMainModule:
  when defined(useServer):
    import std / [os,exitProcs,strformat,osproc]
    const serverPath = currentSourcePath.parentDir / "test_concurrent_server.nim"
    const serverBinPath = currentSourcePath.parentDir / "test_concurrent_server"

    var r = execCmdEx(fmt"nim c -d:release --hints:off {serverPath}", options = {poUsePath,poStdErrToStdOut})
    if r.exitCode != 0:
      echo r.output
    doAssert r.exitCode == 0
    const opts = {poUsePath, poDaemon, poStdErrToStdOut}
    var server = startProcess(serverBinPath,options=opts)
    exitProcs.addExitProc proc() = server.terminate()
    sleep(1000)
  var fails :int 
  proc threadFunc(fails:ptr int) {.thread.} =
    let client = newAsyncHttpClient()
    try:
      let r =  waitFor client.getContent("http://127.0.0.1:8080")
    except Exception as e:
      acquire(L)
      echo e.msg
      release(L)
      fails[].inc
    
    client.close()
  initLock(L)
  var futs = newSeq[Thread[ptr int]](threadsNum)
  for i in 0 ..< threadsNum:
    createThread(futs[i], threadFunc,fails.addr)
  joinThreads(futs)
  echo "total:" & $threadsNum & " fails:" & $fails
  deinitLock(L)