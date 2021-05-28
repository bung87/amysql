

import scorper
import scorper / http / streamclient

let threadsNum = 512

when isMainModule:
  when defined(useServer):
    import std / [exitProcs, strformat,osproc]
    const serverPath = currentSourcePath.parentDir / "test_concurrent_server.nim"
    const serverBinPath = currentSourcePath.parentDir / "test_concurrent_server"

    var r = execCmdEx(fmt"nim c -d:release -d:ChronosAsync --hints:off {serverPath}", options = {poUsePath})
    doAssert r.exitCode == 0
    const opts = {poUsePath, poDaemon, poStdErrToStdOut}
    var server = startProcess(serverBinPath,options=opts)

    sleep(1000)

  proc asyncProc():Future[AsyncResponse] {.async.} =
    var client = newAsyncHttpClient()
    try:
      result = await  client.get("http://127.0.0.1:8080")
      echo await result.readBody()
    except Exception as e:
      echo e.msg
  
  var futs = newSeqOfCap[Future[AsyncResponse]](threadsNum)
  for i in 1 .. threadsNum:
    futs.add(asyncProc())
  try:
    discard waitFor all(futs)
  except Exception as e:
    echo e.msg