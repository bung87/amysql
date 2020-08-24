import asyncnet
import ./private/cap
import regex
import ./private/utils
import strutils, parseutils
import options

type
  Version* = distinct string
  Connection* = ref ConnectionObj
  ConnectionObj* = object of RootObj
    socket*: AsyncSocket               # Bytestream connection
    packet_number*: uint8              # Next expected seq number (mod-256)

    # Information from the connection setup
    server_version*: string
    thread_id*: uint32
    server_caps*: set[Cap]

    # Other connection parameters
    client_caps*: set[Cap]

    databaseVersion: Version
    isMaria: Option[bool]

proc `$`*(ver: Version): string {.borrow.}

converter toBoolean*(ver: Version): bool = ($ver).len > 0

proc `==`*(ver: Version, ver2: Version): bool = $ver == $ver2

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

proc versionString(fullVersionString: string): string =
  # 5.7.27-0ubuntu0.18.04.1
  var m: regex.RegexMatch
  discard fullVersionString.match(re"^(?:5\.5\.5-)?(\d+\.\d+\.\d+)", m)
  fullVersionString[m.group(1)[0]]

proc getDatabaseVersion*(self: Connection): Version {.
                         cachedProperty: "databaseVersion".} =
  let versionString = versionString(self.server_version)
  Version(versionString)

proc mariadb*(self: Connection): bool =
  if self.isMaria.isNone:
    self.isMaria = some self.server_version.contains(re"(?i)mariadb")
  return self.isMaria.get