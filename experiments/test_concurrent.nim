import osproc
import os
import strformat
import asyncdispatch, httpclient
import std / [exitProcs]
# import cpuinfo, math

const serverPath = currentSourcePath.parentDir / "test_concurrent_server.nim"

const serverBinPath = currentSourcePath.parentDir / "test_concurrent_server"

let threadsNum = 512 #nextPowerOfTwo(cpuinfo.countProcessors())

when isMainModule:
  when defined(useServer):
    var r = execCmdEx(fmt"nim c -d:release -d:ChronosAsync --hints:off {serverPath}", options = {poUsePath})
    doAssert r.exitCode == 0
    const opts = {poUsePath, poDaemon, poStdErrToStdOut}
    var server = startProcess(serverBinPath,options=opts)

    sleep(1000)

  # proc asyncProc():Future[string] {.async.} =
  #   var client = newAsyncHttpClient()
  #   try:
  #     result = await  client.getContent("http://127.0.0.1:8080")
  #     echo result
  #   except Exception as e:
  #     echo e.msg
  
  # var futs = newSeqOfCap[Future[string]](threadsNum)
  # for i in 1 .. threadsNum:
  #   futs.add(asyncProc())
  # try:
  #   discard waitFor all(futs)
  # except Exception as e:
  #   echo e.msg
  #   server.terminate()
  proc threadFunc() {.thread.} =
    var client = newAsyncHttpClient()
    try:
      let r =  waitFor client.getContent("http://127.0.0.1:8080")
      echo r
    except Exception as e:
      echo e.msg
  
  var futs = newSeq[Thread[void]](threadsNum)
  for i in 0 ..< threadsNum:
    createThread(futs[i], threadFunc)
  joinThreads(futs)
  # server.terminate()
  when defined(useServer):
    exitProcs.addExitProc proc() = server.terminate()
  