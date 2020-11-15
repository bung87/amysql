{.used.}
import logging
export logging

var consoleLog = newConsoleLogger()
addHandler(consoleLog)
when defined(release):  setLogFilter(lvlInfo)