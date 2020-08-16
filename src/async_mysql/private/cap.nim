type
  # These correspond to the bits in the capability words,
  # and the CLIENT_FOO_BAR definitions in mysql. We rely on
  # Nim's set representation being compatible with the
  # C bit-masking convention.
  # https://dev.mysql.com/doc/dev/mysql-server/8.0.18/group__group__cs__capabilities__flags.html
  Cap* {.pure.} = enum
    longPassword = 0 # new more secure passwords
    foundRows = 1 # Found instead of affected rows
    longFlag = 2 # Get all column flags
    connectWithDb = 3 # One can specify db on connect
    noSchema = 4 # Don't allow database.table.column
    compress = 5 # Can use compression protocol
    odbc = 6 # Odbc client
    localFiles = 7 # Can use LOAD DATA LOCAL
    ignoreSpace = 8 # Ignore spaces before '('
    protocol41 = 9 # New 4.1 protocol
    interactive = 10 # This is an interactive client
    ssl = 11 # Switch to SSL after handshake
    ignoreSigpipe = 12  # IGNORE sigpipes
    transactions = 13 # Client knows about transactions
    reserved = 14  # Old flag for 4.1 protocol
    secureConnection = 15  # Old flag for 4.1 authentication
    multiStatements = 16  # Enable/disable multi-stmt support
    multiResults = 17  # Enable/disable multi-results
    psMultiResults = 18  # Multi-results in PS-protocol
    pluginAuth = 19  # Client supports plugin authentication
    connectAttrs = 20  # Client supports connection attributes
    pluginAuthLenencClientData = 21  # Enable authentication response packet to be larger than 255 bytes.
    canHandleExpiredPasswords = 22  # Don't close the connection for a connection with expired password.
    sessionTrack = 23
    deprecateEof = 24  # Client no longer needs EOF packet
    optionalResultsetMetaData = 25 
    zstdCompressionAlgorithm = 26
    capabilityExtension = 29
    sslVerifyServerCert = 30
    rememberOptions = 31