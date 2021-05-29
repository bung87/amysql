
# import scorper
# import scorper / http / streamclient

import httpclient
import locks

var L: Lock

let threadsNum = 125

when isMainModule:
  when defined(useServer):
    import std / [exitProcs,strformat,osproc]
    const serverPath = currentSourcePath.parentDir / "test_concurrent_server.nim"
    const serverBinPath = currentSourcePath.parentDir / "test_concurrent_server"

    var r = execCmdEx(fmt"nim c -d:release -d:ChronosAsync --hints:off {serverPath}", options = {poUsePath})
    doAssert r.exitCode == 0
    const opts = {poUsePath, poDaemon, poStdErrToStdOut}
    var server = startProcess(serverBinPath,options=opts)
    exitProcs.addExitProc proc() = server.terminate()
    sleep(1000)
  var fails :int 
  proc threadFunc(fails:ptr int) {.thread.} =
    let client = newHttpClient()
    try:
      let r = client.getContent("http://127.0.0.1:8080")
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
  echo "fails:" & $fails
  deinitLock(L)