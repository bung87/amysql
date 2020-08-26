import amysql, asyncdispatch
import unittest
import net

const database_name = "test"
const port: int = 3306
const host_name = "localhost"
const user_name = "test_user"
const pass_word = "123456"
const ssl: bool = false
const verbose: bool = false

proc geometryTests(conn: Connection,data: string): Future[void] {.async.} =
  discard await conn.rawQuery("drop table if exists geotest")
  discard await conn.rawQuery("CREATE TABLE geotest (g GEOMETRY) ENGINE=aria")

  # Insert values using the binary protocol
  let insrow = await conn.prepare("INSERT INTO geotest (g) VALUES (?)")
  let d1 = newMyGeometry(data)

  discard await conn.query(insrow, d1)
  await conn.finalize(insrow)

  # Read them back using the text protocol
  let r1 = await conn.rawQuery("SELECT * FROM geotest")
  # check r1.rows[0][0] == "2020-01-01"
  echo r1.rows
  echo r1.rows[0][0]
  

  # Now read them back using the binary protocol
  let rdtab = await conn.prepare("SELECT * FROM geotest")
  let r2 = await conn.query(rdtab)
  check r2.rows[0][0] == d1

  await conn.finalize(rdtab)

  discard await conn.rawQuery("drop table geotest")

proc buildString(data:openarray[SomeInteger]): string = 
  let dataLen = data.len
  result = newString(data.len)
  for i in 0 ..< dataLen:
    result[i] = data[i].char

var conn:Connection
suite "geometry":
  setup:
    conn =  waitFor amysql.open(host_name,user_name,pass_word,database_name)
  teardown:
    waitFor conn.close()
  test "Geometry":
    let data = buildString [0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63]
    waitFor conn.geometryTests(data)

  test "Point":
    let data = buildString [0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63 ]
    waitFor conn.geometryTests(data)

  test "LineString":
    let data = buildString [0, 0, 0, 0, 1, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 64  ]
    waitFor conn.geometryTests(data)

  test "Polygon":
    let data = buildString [0, 0, 0, 0, 1, 3, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 ]
    waitFor conn.geometryTests(data)
  
  test "MultiPoint":
    let data = buildString [0, 0, 0, 0, 1, 4, 0, 0, 0, 3, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 64, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 8, 64 ]
    waitFor conn.geometryTests(data)
  
  test "MultiLineString":
    let data = buildString [0, 0, 0, 0, 1, 5, 0, 0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 52, 64, 0, 0, 0, 0, 0, 0, 52, 64, 1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 46, 64, 0, 0, 0, 0, 0, 0, 46, 64, 0, 0, 0, 0, 0, 0, 62, 64, 0, 0, 0, 0, 0, 0, 46, 64 ]
    waitFor conn.geometryTests(data)

  test "MultiPolygon":
    let data = buildString [0, 0, 0, 0, 1, 6, 0, 0, 0, 2, 0, 0, 0, 1, 3, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 3, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 64, 0, 0, 0, 0, 0, 0, 20, 64, 0, 0, 0, 0, 0, 0, 28, 64, 0, 0, 0, 0, 0, 0, 20, 64, 0, 0, 0, 0, 0, 0, 28, 64, 0, 0, 0, 0, 0, 0, 28, 64, 0, 0, 0, 0, 0, 0, 20, 64, 0, 0, 0, 0, 0, 0, 28, 64, 0, 0, 0, 0, 0, 0, 20, 64, 0, 0, 0, 0, 0, 0, 20, 64]
    waitFor conn.geometryTests(data)

  test "GeometryCollection":
    let data = buildString [0, 0, 0, 0, 1, 7, 0, 0, 0, 3, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 36, 64, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 62, 64, 0, 0, 0, 0, 0, 0, 62, 64, 1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 46, 64, 0, 0, 0, 0, 0, 0, 46, 64, 0, 0, 0, 0, 0, 0, 52, 64, 0, 0, 0, 0, 0, 0, 52, 64]
    waitFor conn.geometryTests(data)

