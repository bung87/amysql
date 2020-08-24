const
  ## Server status used by Client/Server Protocol.
  SERVER_STATUS_IN_TRANS*                 = 1
    ## Is raised when a multi-statement transaction has been started, either 
    ## explicitly, by means of BEGIN or COMMIT AND CHAIN, or implicitly, by 
    ## the first transactional statement, when autocommit=off.
  SERVER_STATUS_AUTOCOMMIT*               = 2
    ## Server in auto_commit mode.
  SERVER_MORE_RESULTS_EXISTS*             = 8
    ## Multi query - next query exists.
  SERVER_QUERY_NO_GOOD_INDEX_USED*        = 16
  SERVER_QUERY_NO_INDEX_USED*             = 32
  SERVER_STATUS_CURSOR_EXISTS*            = 64
    ## The server was able to fulfill the clients request and opened a read-only 
    ## non-scrollable cursor for a query.
  SERVER_STATUS_LAST_ROW_SENT*            = 128
    ## This flag is sent when a read-only cursor is exhausted, in reply to COM_STMT_FETCH command.
  SERVER_STATUS_DB_DROPPED*               = 256
    ## A database was dropped.
  SERVER_STATUS_NO_BACKSLASH_ESCAPES*     = 512
  SERVER_STATUS_METADATA_CHANGED*         = 1024
    ## Sent to the client if after a prepared statement reprepare we discovered 
    ## that the new statement returns a different number of result set fields.
  SERVER_QUERY_WAS_SLOW*                  = 2048
  SERVER_PS_OUT_PARAMS*                   = 4096
    ## To mark ResultSet containing output parameter values.
  SERVER_STATUS_IN_TRANS_READONLY*        = 8192
    ## Set at the same time as SERVER_STATUS_IN_TRANS if the started multi-statement transaction 
    ## is a read-only transaction.
  SERVER_SESSION_STATE_CHANGED*           = 16384
    ## This status flag, when on, implies that one of the state information has changed on 
    ## the server because of the execution of the last statement.