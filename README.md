# async_mysql  [![Build Status](https://travis-ci.org/bung87/async_mysql.svg?branch=master)](https://travis-ci.org/bung87/async_mysql)  [![Test status](https://github.com/bung87/async_mysql/workflows/test/badge.svg)](https://github.com/bung87/async_mysql/actions)  

This module implements (a subset of) the MySQL/MariaDB client protocol based on asyncnet and asyncdispatch.

## Goals

The goals of this project are:

1. **Similar API to Nim's std db lib** 
2. **Async:** All operations must be truly asynchronous whenever possible.
3. **High performance:** Avoid unnecessary allocations and copies when reading data.
4. **Managed:** Managed code only, no native code.
6. **Independent:** This is a clean-room reimplementation of the [MySQL Protocol](https://dev.mysql.com/doc/internals/en/client-server-protocol.html), not based on C lib.