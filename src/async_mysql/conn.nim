import asyncnet
import ./private/cap
type
  Connection* = ref ConnectionObj
  ConnectionObj = object of RootObj
    socket*: AsyncSocket               # Bytestream connection
    packet_number*: uint8              # Next expected seq number (mod-256)

    # Information from the connection setup
    server_version*: string
    thread_id*: uint32
    server_caps*: set[Cap]

    # Other connection parameters
    client_caps*: set[Cap]