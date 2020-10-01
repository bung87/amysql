import unittest
import amysql/private/auth
include ./utils
proc mysql_native_password(s: string,p: string): string = hexstr(scramble_native_password(s,p))

suite "test password encryption":
  test "mysql_native_password":
    check mysql_native_password("L\\i{NQ09k2W>p<yk/DK+","foo") == "f828cd1387160a4c920f6c109d37285d281f7c85"
    check mysql_native_password("<G.N}OR-(~e^+VQtrao-","aaaaaaaaaaaaaaaaaaaabbbbbbbbbb") == "78797fae31fc733107e778ee36e124436761bddc"