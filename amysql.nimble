# Package

version       = "0.2.1"
author        = "bung87"
description   = "Async MySQL Connector write in pure Nim."
license       = "MIT"
srcDir        = "src"

task docs,"":
  exec "nim doc --project src/amysql/index.nim"

task ghpage,"gh page":
  cd "src/htmldocs" 
  exec "git init"
  exec "git add ."
  exec "git config user.name \"bung87\""
  exec "git config user.email \"crc32@qq.com\""
  exec "git commit -m \"docs(docs): update gh-pages\""
  let url = "\"https://bung87@github.com/bung87/amysql.git\""
  exec "git push --force --quiet " & url & " master:gh-pages"


# Dependencies
requires "nim >= 1.3.1" # await inside template needs
requires "nimcrypto"
requires "regex"
