
# InnoDB offers all four transaction isolation levels described by the SQL:1992 standard:
# READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, and SERIALIZABLE. The default isolation level for InnoDB is REPEATABLE READ.
# see https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-isolation-levels.html
type
  IsolationLevel* = enum
    levelDefault = "Default"
    LevelReadUncommitted = "Read Uncommitted"
    LevelReadCommitted = "Read Committed"
    LevelWriteCommitted = "Write Committed"
    LevelRepeatableRead = "Repeatable Read"
    LevelSnapshot = "Snapshot"
    LevelSerializable = "Serializable"
    LevelLinearizable = "Linearizable"
    
func isolateLevel*(level: int): string {.inline.} =
  "IsolationLevel(" & $level & ")"

