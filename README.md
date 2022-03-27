# amysql  [![Build Status](https://travis-ci.org/bung87/amysql.svg?branch=master)](https://travis-ci.org/bung87/amysql)  

`amysql` implements (a subset of) the MySQL/MariaDB client protocol based on asyncnet and asyncdispatch.  

`amysql` implements both the **text protocol** (send a simple string query, get back results as strings) and the **binary protocol** (get a prepared statement handle from a string with placeholders; send a set of value bindings, get back results as various datatypes approximating what the server is using).  

`amysql` implements async connection pool(`amysql/async_pool`) and a threaded pool(`amysql/db_pool`).  

## Usage  

### Compile flags  

```
async macro:
ChronosAsync 

chronos use its own async macro will conflicts with std async macro , use this flag if your project use chronos.  

socket io:
const ReadTimeOut {.intdefine.} = 30_000
const WriteTimeOut {.intdefine.} = 60_000  

idle check:
const TestWhileIdle* {.booldefine.} = true
const MinEvictableIdleTime {.intdefine.} = 60_0000
const TimeBetweenEvictionRuns {.intdefine.} = 30_000  

compression:  
const mysql_compression_mode {.booldefine.} = false
const ZstdCompressionLevel {.intdefine.} = 3

pool:
const ResetConnection* {.booldefine.} = true # reset session when reuse 

```
## Goals

The goals of this project are:

1. **Similar API to Nim's std db lib** 
2. **Async:** All operations must be truly asynchronous whenever possible.
3. **High performance:** Avoid unnecessary allocations and copies when reading data.
4. **Managed:** Managed code only, no native code.
6. **Independent:** This is a clean-room reimplementation of the [MySQL Protocol](https://dev.mysql.com/doc/internals/en/client-server-protocol.html), not based on C lib.  

## Usage  

### connection  

open connection by passing host, username, password, database  
last optional parameter is `connectAttrs: Table[system.string, system.string]`

``` nim
let conn = await amysql.open("localhost:3306",username,password,database)
await conn.close()
```
open connection by passing dsn  

``` nim
let conn = await amysql.open(fmt"mysql://{username}:{password}@{host}/{database}?charset=utf8mb4,utf8&sql_mode=TRADITIONAL")
await conn.close()
```

open connection pool  

``` nim
import amysql/async_pool
import amysql
var pool = await newAsyncPool(host,
    username,
    password,
    database,2)
let conIdx = await pool.getFreeConnIdx()
let conn = pool.getFreeConn(conIdx)
let insrow = await conn.prepare("insert into `num_tests` (s, u8, s8, u, i, b) values (?, ?, ?, ?, ?, ?)")
discard await conn.query(insrow, "one", 1, 1, 1, 1, 1)
discard await conn.query(insrow, "max", 255, 127, 4294967295, 2147483647, 9223372036854775807'u64)
discard await conn.query(insrow, "min", 0, -128, 0, -2147483648, (-9223372036854775807'i64 - 1))
discard await conn.query(insrow, "foo", 128, -127, 256, -32767, -32768)
await conn.finalize(insrow)
pool.returnConn(conIdx)
```

### database  

get current database  

``` nim
await conn.getCurrentDatabase()
```

select database  

``` nim 
await conn.selectDatabase()
```

### exec and query  

**rawQuery** api return every row in form `seq[string` , `query`  

**query** api return every row in variant bounded types  

``` nim  
type 
  ColumnDefinition* {.final.} = object 
    catalog*     : string
    schema*      : string
    table*       : string
    origTable*  : string
    name*        : string
    origName*   : string
    charset*      : int16
    length*      : uint32
    columnType* : FieldType
    flags*       : set[FieldFlag]
    decimals*    : int
  
  ResultSet*[T] {.final.} = object 
    status*     : ResponseOK
    columns*    : seq[ColumnDefinition]
    rows*       : seq[seq[T]]
```
`rawQuery` and `rawExec` returns `Future[ResultSet[system.string]]` , `rawExec` has empty rows. 

``` nim
discard await conn.rawExec("drop table if exists num_tests")
```

`rawQuery` has optional parameter `onlyFirst:bool` which determine whether only fetch first row.

``` nim
let r1 = await conn.rawQuery("select s, u8, s8, u, i, b from num_tests order by u8 asc")
```  

prepare query returns `Future[amysql.SqlPrepared]` which can be used with `query` multiple times, after `query` executed don't forget call `conn.finalize(sqlPrepared)`  

``` nim 

let insrow = await conn.prepare("insert into `num_tests` (s, u8, s8, u, i, b) values (?, ?, ?, ?, ?, ?)")
discard await conn.query(insrow, "one", 1, 1, 1, 1, 1)
discard await conn.query(insrow, "max", 255, 127, 4294967295, 2147483647, 9223372036854775807'u64)

await conn.finalize(insrow)
```

Retrieves a single row

``` nim
let r5 = await conn.getRow(sql"select s, u8, s8, u, i, b from num_tests order by u8 asc")
check r5 == @["min", "0", "-128", "0", "-2147483648", "-9223372036854775808"]
```

executes the query and returns the first column of the first row
``` nim
checkpoint "getValue"
let r6 = await conn.getValue(sql"select s, u8, s8, u, i, b from num_tests order by u8 asc")
check r6 == "min"
``` 
### column types 

float and double  

``` nim
discard await conn.rawQuery("CREATE TABLE `float_test`(`fla` FLOAT,`flb` FLOAT,`dba` DOUBLE(10,2),`dbb` DOUBLE(10,2))")

# Insert values using the binary protocol
let insrow = await conn.prepare("INSERT INTO `float_test` values (?,?,?,?)")
discard await conn.query(insrow, 1.2'f32, 1.2'f32, 1.2'f64, 1.2'f64)
await conn.finalize(insrow)
```

datetime  

``` nim 
import times
discard await conn.rawQuery("CREATE TABLE test_dt(col DATETIME NOT NULL)")

# Insert values using the binary protocol
let insrow = await conn.prepare("INSERT INTO test_dt (col) VALUES (?),(?),(?)")
let d1 = initDateTime(1,1.Month,2020,10,10,10,utc())
let d2 = initDateTime(1,1.Month,2020,4,40,10,utc())
let d3 = initDateTime(1,1.Month,2020,18,10,10,utc())
discard await conn.query(insrow, d1, d2, d3)
await conn.finalize(insrow)
```
timestamp  

``` nim
discard await conn.rawQuery("CREATE TABLE test_dt(col TIMESTAMP NOT NULL)")

# Insert values using the binary protocol
let insrow = await conn.prepare("INSERT INTO test_dt (col) VALUES (?),(?),(?)")
let d1 = initDateTime(1,1.Month,2020,10,10,10,utc())
let d2 = initDateTime(1,1.Month,2020,4,40,10,utc())
let d3 = initDateTime(1,1.Month,2020,18,10,10,utc())
discard await conn.query(insrow, d1.toTime, d2.toTime, d3.toTime)
await conn.finalize(insrow)
```

date  

``` nim
discard await conn.rawQuery("CREATE TABLE test_dt(col DATE NOT NULL)")

# Insert values using the binary protocol
let insrow = await conn.prepare("INSERT INTO test_dt (col) VALUES (?),(?)")
let d1 = initDate(1,1.Month,2020)
let d2 = initDate(31,12.Month,2020)
discard await conn.query(insrow, d1, d2)
await conn.finalize(insrow)
```

time

``` nim 
import times  
discard await conn.rawQuery("CREATE TABLE test_dt(start_at TIME,end_at TIME)")

# Insert values using the binary protocol
let insrow = await conn.prepare("INSERT INTO test_dt (start_at,end_at) VALUES (?,?)")
let d1 = initDuration(hours=8)
let d2 = initDuration(hours=10)
discard await conn.query(insrow, d1, d2)
await conn.finalize(insrow)
```

json  

``` nim
import json
discard await conn.rawQuery("CREATE TABLE test_dt(col JSON)")

# Insert values using the binary protocol
let insrow = await conn.prepare("INSERT INTO test_dt (col) VALUES (?),(?)")
let d1 = parseJson("""
{
"name": "Nimmer",
"age": 21
}
""")
let d2 = parseJson("[1, 2, 3, 4]")
discard await conn.query(insrow, d1, d2)
await conn.finalize(insrow)

# Read them back using the text protocol
let r1 = await conn.rawQuery("SELECT * FROM test_dt")
# mysql return `{"age": 21, "name": "Nimmer"}`
# $d1 return `{"name":"Nimmer","age":21}`
check r1.rows[0][0] == conn.sqlFormat(d1)
check r1.rows[1][0] == conn.sqlFormat(d2)
```

geometry  

``` nim
discard await conn.rawQuery("CREATE TABLE geotest (g GEOMETRY)")

# Insert values using the binary protocol
let insrow = await conn.prepare("INSERT INTO geotest (g) VALUES (?)")
let d1 = newMyGeometry(data)

discard await conn.query(insrow, d1)
await conn.finalize(insrow)

# Now read them back using the binary protocol
let rdtab = await conn.prepare("SELECT * FROM geotest")
let r2 = await conn.query(rdtab)
check r2.rows[0][0] == d1

await conn.finalize(rdtab)
```

load data from csv  

``` nim

const CreateTable = """
  CREATE TABLE IF NOT EXISTS `person`(
   `id` INT UNSIGNED AUTO_INCREMENT,
   `fname` VARCHAR(100) NOT NULL,
   `lname` VARCHAR(40) NOT NULL,
   PRIMARY KEY ( `id` )
  ) ENGINE=MyISAM DEFAULT CHARSET=utf8
  """
const EnableLocalInfileData = "SET GLOBAL local_infile = true"
discard await conn.tryQuery(SqlQuery EnableLocalInfileData)
discard await conn.rawQuery(CreateTable)
let filename = currentSourcePath.parentDir / "localinfile.csv"
let r = await conn.rawQuery(fmt"LOAD DATA LOCAL INFILE '{filename}' INTO TABLE person")
```  

## Testing  

platform: Linux and OSX  

mysql: 5.7 8.0  

mariadb: 10  

## TODO  

- [ ] Finish caching_sha2_password_auth.  
- [ ] Testing ssl mode ,unix socket mode.  
- [x] mutiple statements and mutilple resultsets.
- [x] compression mode (zstd)  
- [x] handle connection options  

## Acknowledgements  

[PyMySQL](https://github.com/PyMySQL/PyMySQL)  

[MySqlConnector](https://github.com/mysql-net/MySqlConnector)

[tulayang](https://github.com/tulayang)'s [asyncmysql](https://github.com/tulayang/asyncmysql) and [mysqlparser](https://github.com/tulayang/mysqlparser)  

[wiml/nim-asyncmysql](https://github.com/wiml/nim-asyncmysql)  

[go-sql-driver/mysql](https://github.com/go-sql-driver/mysql)  

## History  

When I starting this project, I have `wiml/nim-asyncmysql` and `asyncmysql` for inspiration, the initial goal is provide basic asynchronous apis compare to Nim std library `db_mysql`'s synchronous apis, `asyncmysql` provide mutiple results feature and callback style apis by design, he map mysql protocol flags to exact to c types, `wiml/nim-asyncmysql` provide asynchronous apis and map flags to enum type and the project design is very obvious to me, so I choose base on `wiml/nim-asyncmysql`.  

When this project ready to use, it provide single statement and single results, I starting considering provide compression api, at that time data recevie as string and passing around procs, I starting store data to `seq[char]` as I dont want decompress data and passing around procs, and it also help me move next step, implements multiple results feature. Compare to `asyncmysql` ,`asyncmysql` use a constant size array buffer to store data, I may change as it so in the future.

Above declaration may not explain "Why this project exists ?", here's some key points.  

When I starting this project `asyncmysql` and `wiml/nim-asyncmysql` have no commits activities for years. `wiml/nim-asyncmysql` even can't compile for current Nim version.  

1. `db_mysql` provide synchronous apis and need external c library.
2. `asyncmysql` provide callback style apis.
3. `wiml/nim-asyncmysql` satisify the design, but need update to current Nim version, more column data type mapping, more features , more client capbilities. 
