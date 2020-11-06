# amysql  [![Build Status](https://travis-ci.org/bung87/amysql.svg?branch=master)](https://travis-ci.org/bung87/amysql)  

`amysql` implements (a subset of) the MySQL/MariaDB client protocol based on asyncnet and asyncdispatch.  

`amysql` implements both the **text protocol** (send a simple string query, get back results as strings) and the **binary protocol** (get a prepared statement handle from a string with placeholders; send a set of value bindings, get back results as various datatypes approximating what the server is using).  

`amysql` implements async connection pool(`amysql/async_pool`) and a threaded pool(`amysql/db_pool`).  

## Usage  

### Compile flags  

```
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
```
## Goals

The goals of this project are:

1. **Similar API to Nim's std db lib** 
2. **Async:** All operations must be truly asynchronous whenever possible.
3. **High performance:** Avoid unnecessary allocations and copies when reading data.
4. **Managed:** Managed code only, no native code.
6. **Independent:** This is a clean-room reimplementation of the [MySQL Protocol](https://dev.mysql.com/doc/internals/en/client-server-protocol.html), not based on C lib.  

## Testing  

platform: Linux and OSX  

mysql: 5.7 8.0  

mariadb: 10  

## TODO  

- [ ] Finish caching_sha2_password_auth.  
- [ ] Testing ssl mode unix socket mode.  
- [ ] mutiple statements and mutilple resultsets.
- [x] compression mode (zstd)  
- [ ] handle connection options  

## Acknowledgements  

[PyMySQL](https://github.com/PyMySQL/PyMySQL)  

[MySqlConnector](https://github.com/mysql-net/MySqlConnector)

[tulayang](https://github.com/tulayang)'s [asyncmysql](https://github.com/tulayang/asyncmysql) and [mysqlparser](https://github.com/tulayang/mysqlparser)  

[wiml/nim-asyncmysql](https://github.com/wiml/nim-asyncmysql)  

[go-sql-driver/mysql](https://github.com/go-sql-driver/mysql)
