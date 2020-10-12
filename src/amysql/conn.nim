
import asyncnet
import ./private/cap
import regex
import ./private/utils
import strutils, parseutils
import options
import tables

import asyncnet

const BasicClientCaps* = { Cap.longPassword, Cap.protocol41, Cap.secureConnection }

type
  Version* = distinct string
  Connection* = ref ConnectionObj
  ConnectionObj* = object of RootObj
    socket*: AsyncSocket               # Bytestream connection
    sequenceId*: uint8              # Next expected seq number (mod-256)

    # Information from the connection setup
    serverVersion*: string
    threadId*: uint32
    serverCaps*: set[Cap]

    # Other connection parameters
    clientCaps*: set[Cap]

    databaseVersion: Option[Version]
    priv_isMaria: Option[bool]

proc `$`*(ver: Version): string {.borrow.}

converter toBoolean*(ver: Version): bool = ($ver).len > 0

proc `<`*(ver: Version, ver2: Version): bool =
  ## This is synced from Nimble's version module.
  # Handling for normal versions such as "0.1.0" or "1.0".
  var sVer = string(ver).split('.')
  var sVer2 = string(ver2).split('.')
  for i in 0..<max(sVer.len, sVer2.len):
    var sVerI = 0
    if i < sVer.len:
      discard parseInt(sVer[i], sVerI)
    var sVerI2 = 0
    if i < sVer2.len:
      discard parseInt(sVer2[i], sVerI2)
    if sVerI < sVerI2:
      return true
    elif sVerI == sVerI2:
      discard
    else:
      return false

proc `==`*(ver: Version, ver2: Version): bool = $ver == $ver2

proc `>`*(ver: Version, ver2: Version): bool = ver2 < ver

proc `<=`*(ver: Version, ver2: Version): bool = ver < ver2 or ver == ver2

proc versionString(fullVersionString: string): string =
  # 5.7.27-0ubuntu0.18.04.1
  if fullVersionString.len == 0:
    return result
  var m: regex.RegexMatch
  discard fullVersionString.find(re"^(?:5\.\d+\.\d+-)?(\d+\.\d+\.\d+)", m)
  fullVersionString[m.group(0)[0]]

proc getDatabaseVersion*(self: Connection): Version {.
                         cachedProperty: "databaseVersion".} =
  let versionString = versionString(self.serverVersion)
  Version(versionString)

proc isMaria*(self: Connection): bool  {.
              cachedProperty: "priv_isMaria".} =
  self.serverVersion.contains(re"(?i)mariadb")

proc `$`*(conn: Connection): string = 
  var tbl:OrderedTable[string,string]
  tbl["serverVersion"] = conn.serverVersion
  tbl["serverCaps"] = $conn.serverCaps
  tbl["clientCaps"] = $conn.clientCaps
  tbl["databaseVersion"] = $conn.getDatabaseVersion()
  tbl["isMaria"] = $conn.isMaria
  $tbl

when isMainModule:
  echo Version("8.0.21") >= Version("8.0")
  echo Version("5.7.30") >= Version("8.0")
  var conn = Connection()
  echo conn