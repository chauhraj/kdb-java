# Interprocess Communication (IPC) Protocol

## Overview
The IPC protocol in kdb+ facilitates communication between processes, allowing for efficient data exchange and remote procedure calls.

## Handshake
- After a client opens a socket to the server, it sends a null-terminated ASCII string `"username:password\N"` where `\N` is a single byte (0…3) representing the client’s capability.
- If the server rejects the credentials, it closes the connection immediately.
- If accepted, the server sends a single-byte response representing the common capability.

## Message Types
### Sync Message
- Sent by the client to request data or execute a function on the server.
- The client waits for a response before proceeding.

### Async Message
- Sent by the client without waiting for a response.
- Useful for non-blocking operations.

## Compression
- Messages are compressed if:
  - Uncompressed data length > 2000 bytes.
  - Connection is not localhost or using UDS.
  - Compressed data is less than half the size of uncompressed data.

## Capabilities
- Capability bytes indicate the features supported by the client and server.
- Common capabilities:
  - `0`: No compression, no timestamp, no timespan, no UUID.
  - `3`: Compression, timestamp, timespan, UUID.

For more detailed information, refer to the [kdb+ IPC documentation](https://code.kx.com/q/basics/ipc/). 