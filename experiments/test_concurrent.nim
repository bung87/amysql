import osproc
import os
import strformat
import asyncdispatch, httpclient
import std / [exitProcs]
const serverPath = currentSourcePath.parentDir / "test_concurrent_server.nim"

const serverBinPath = currentSourcePath.parentDir / "test_concurrent_server"

when isMainModule:
  var r = execCmdEx(fmt"nim c --hints:off {serverPath}", options = {poUsePath})
  doAssert r.exitCode == 0
  const opts = {poUsePath, poDaemon, poStdErrToStdOut}
  var server = startProcess(serverBinPath,options=opts)

  sleep(1000)
 
  proc asyncProc():Future[string] {.async.} =
    var client = newAsyncHttpClient()
    return await  client.getContent("http://127.0.0.1:8080")
 
  try:
    waitFor asyncProc() and asyncProc()
  except Exception as e:
    echo e.msg
    server.terminate()

  exitProcs.addExitProc proc() = server.terminate()
  exitProcs.addExitProc proc() = server.close()
