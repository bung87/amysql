
when defined(ChronosAsync):
  import chronos
else:
  import asyncnet
import ./private/cap
import regex
import ./private/utils
import strutils, parseutils
import options
import tables
import times

const BasicClientCaps* = { Cap.longPassword, Cap.protocol41, Cap.secureConnection }
const TestWhileIdle* {.booldefine.} = true
const MysqlBufSize* = 1024

type
  Version* = distinct string
  Connection* = ref ConnectionObj
  ConnectionObj* = object of RootObj
    when defined(ChronosAsync):
      transp*: StreamTransport
    else:
      transp*: AsyncSocket               # Bytestream connection
    sequenceId*: uint8              # Next expected seq number (mod-256)
    when defined(mysql_compression_mode):
      compressedSequenceId*: uint8
    # Information from the connection setup
    serverVersion*: string
    threadId*: uint32
    serverCaps*: set[Cap]

    # Other connection parameters
    clientCaps*: set[Cap]

    databaseVersion: Option[Version]
    priv_isMaria: Option[bool]
    authenticated*: bool
    when TestWhileIdle:
      when not defined(ChronosAsync):
        lastOperationTime*: DateTime
      else:
        lastOperationTime*: Moment
    buf*: seq[char]
    bufLen*: int
    payloadLen*: int
    bufPos*: int
    # remainPacketLen*: int
    fullPacketLen*: int
    curPacketLen*: int
    # curPayloadLen*: int
    connectAttrs*:Table[string,string]
    hasMoreResults*: bool

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
  tbl["hasMoreResults"] = $conn.hasMoreResults
  $tbl

proc zstdAvailable*(conn: Connection): bool =
  # https://dev.mysql.com/worklog/task/?id=12475
  # If compression method is set to "zlib" then CLIENT_COMPRESS capability flag
  # will be enabled else if set to "zstd" then new capability flag
  # CLIENT_ZSTD_COMPRESSION_ALGORITHM will be enabled.
  # Note: When --compression-algorithms is set without --compress option then 
  # protocol is still enabled with compression.
  const compress_zstd = { Cap.zstdCompressionAlgorithm }
  return compress_zstd <= conn.serverCaps and compress_zstd <= conn.clientCaps

proc use_zstd*(conn: Connection): bool = conn.zstdAvailable() and conn.authenticated

proc firstByte*(conn: Connection): lent char =
  conn.buf[conn.bufPos.int]

proc remainPacketLen*(conn: Connection):int =
  conn.fullPacketLen - conn.bufPos

proc zeroPos*(conn: Connection) =
  conn.bufPos = 0

proc incPos*(conn: Connection; count = 1) =
  conn.bufPos.inc count
  # conn.remainPacketLen.dec count + 1

proc getPayloadLen*(conn: Connection): int = 
  result = int32( uint32(conn.buf[conn.bufPos]) or (uint32(conn.buf[conn.bufPos + 1]) shl 8) or (uint32(conn.buf[conn.bufPos + 2]) shl 16) )

proc resetPacketLen*(conn: Connection) =
  conn.curPacketLen = conn.getPayloadLen + 4
  conn.incPos(4)

proc printBufInfo*(conn: Connection) =
  echo "============================"
  echo "conn.bufPos:" & $conn.bufPos
  echo "conn.fullPacketLen:" & $conn.fullPacketLen
  echo "conn.bufLen:" & $conn.bufLen
  echo "conn.curPacketLen:" & $conn.curPacketLen
  echo "conn.remainPacketLen:" & $conn.remainPacketLen
  # echo repr conn.buf
  echo "============================"

when isMainModule:
  echo Version("8.0.21") >= Version("8.0")
  echo Version("5.7.30") >= Version("8.0")
  var conn = Connection()
  echo conn