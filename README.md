# amysql  [![Build Status](https://travis-ci.org/bung87/amysql.svg?branch=master)](https://travis-ci.org/bung87/amysql)  

`amysql` implements (a subset of) the MySQL/MariaDB client protocol based on asyncnet and asyncdispatch.  

`amysql` implements both the **text protocol** (send a simple string query, get back results as strings) and the **binary protocol** (get a prepared statement handle from a string with placeholders; send a set of value bindings, get back results as various datatypes approximating what the server is using).  

## Goals

The goals of this project are:

1. **Similar API to Nim's std db lib** 
2. **Async:** All operations must be truly asynchronous whenever possible.
3. **High performance:** Avoid unnecessary allocations and copies when reading data.
4. **Managed:** Managed code only, no native code.
6. **Independent:** This is a clean-room reimplementation of the [MySQL Protocol](https://dev.mysql.com/doc/internals/en/client-server-protocol.html), not based on C lib.  

## Testing  

mysql 5.7 8.0  

mariadb 10  

## TODO  

- [ ] Finish caching_sha2_password_auth.  
- [ ] Testing ssl mode unix socket mode.  
- [ ] thread pool.  
- [ ] mutiple statements and mutilple resultsets.
- [ ] compression mode  
- [ ] handle connection options  
