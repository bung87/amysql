
# import scorper
# import scorper / http / streamclient

import httpclient
let threadsNum = 100

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

  proc threadFunc() {.thread.} =
    let
      client = newHttpClient()
    let r = client.get("http://127.0.0.1:8080")
    # echo r.body
    client.close()

  var futs = newSeq[Thread[void]](threadsNum)
  for i in 0 ..< threadsNum:
    createThread(futs[i], threadFunc)
  joinThreads(futs)