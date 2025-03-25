# KDB Java Client

## Overview

The `c.java` file is a Java client for interfacing with a KDB+ process. It acts as a serializer/deserializer of Java types to/from the KDB+ IPC wire format, enabling remote method invocation in KDB+ via TCP/IP. This client allows for synchronous and asynchronous communication with a KDB+ server.

## IPC Protocol

The Inter-Process Communication (IPC) protocol used by `c.java` facilitates communication between the Java client and the KDB+ server. It supports both synchronous and asynchronous message exchanges, allowing for efficient data transfer and remote procedure calls.

### Key Features:
- **Synchronous Communication**: Allows the client to send a request and wait for a response.
- **Asynchronous Communication**: Enables the client to send a request without waiting for a response, allowing for non-blocking operations.
- **Data Serialization**: Converts Java objects into a format suitable for transmission over the network.
- **Data Deserialization**: Converts received data back into Java objects.

## Supported Data Types

The following table lists the data types supported by `c.java` and their protocol representations:

| Java Type        | KDB+ Type | Protocol Representation | Communication Details |
|------------------|-----------|-------------------------|-----------------------|
| `boolean`        | `boolean` | `0b`                    | Single byte, 0 or 1   |
| `byte`           | `byte`    | `0x00`                  | Single byte value     |
| `short`          | `short`   | `0h`                    | 2-byte integer        |
| `int`            | `int`     | `0i`                    | 4-byte integer        |
| `long`           | `long`    | `0j`                    | 8-byte integer        |
| `float`          | `real`    | `0e`                    | 4-byte IEEE 754 float |
| `double`         | `float`   | `0f`                    | 8-byte IEEE 754 float |
| `char`           | `char`    | `" "`                  | Single character      |
| `String`         | `symbol`  | `""`                   | Null-terminated string|
| `LocalDate`      | `date`    | `2000.01.01`            | Days since 2000.01.01 |
| `LocalTime`      | `time`    | `00:00:00.000`          | Milliseconds since midnight |
| `LocalDateTime`  | `datetime`| `2000.01.01T00:00:00`   | Seconds since 2000.01.01 |
| `List`           | `list`    | `()`                    | Sequence of elements  |
| `Dictionary`     | `dict`    | `!`                     | Key-value pairs       |
| `Table`          | `table`   | `+`                     | Columnar data structure |

## Database Description

KDB+ is a high-performance, columnar database designed for time-series data and complex analytics. It is widely used in financial services for real-time data processing and analysis. The `c.java` client provides a robust interface for connecting to a KDB+ server, allowing Java applications to leverage the powerful capabilities of KDB+ for data storage and retrieval.

For more information on KDB+ and its features, visit the [official documentation](https://code.kx.com/q/). 

## Query Structure

When making a query to the KDB backend, the communication involves a header and a payload. Here's how they are structured:

### Header
The header is a fixed-size structure that contains metadata about the message being sent. It typically includes:
- **Message Type**: Indicates whether the message is a query, response, or error.
- **Message Size**: The total size of the message, including the header and payload.
- **Protocol Version**: The version of the IPC protocol being used.
- **Compression Flag**: Indicates if the payload is compressed.

### Payload
The payload contains the actual data or query being sent to the KDB server. It is serialized according to the KDB+ IPC protocol and can include:
- **Query String**: The KDB+ query to be executed.
- **Parameters**: Any parameters required by the query.
- **Data**: Additional data needed for the query execution.

### Constructing Header and Payload
To construct a header and payload:
1. **Header**: Create a byte array of fixed size, populate it with the necessary metadata.
2. **Payload**: Serialize the query and parameters into a byte array.
3. **Combine**: Concatenate the header and payload byte arrays to form the complete message.

This structure ensures efficient and reliable communication with the KDB backend, allowing for precise query execution and data retrieval. 

### Header Details

The header is a fixed-size structure with a total length of 8 bytes. It includes the following components:

- **Endianness (1 byte)**: Indicates the byte order (big-endian or little-endian).
- **Message Type (1 byte)**: Indicates the type of message (e.g., query, response, error).
- **Message Size (4 bytes)**: The total size of the message, including the header and payload, represented in big-endian order.
- **Protocol Version (1 byte)**: The version of the IPC protocol being used.
- **Compression Flag (1 byte)**: Indicates if the payload is compressed.

### Buffer Construction

To construct the header, create a byte array of 8 bytes and populate it as follows:

- **Byte 0**: Endianness
- **Byte 1**: Message Type
- **Bytes 2-5**: Message Size (big-endian)
- **Byte 6**: Protocol Version
- **Byte 7**: Compression Flag

This header structure is essential for ensuring that the KDB server correctly interprets the incoming message, allowing for efficient and reliable communication. 

### Payload Details

The payload is a variable-length structure that contains the actual data or query being sent to the KDB server. It includes the following components:

- **Query String**: The KDB+ query to be executed, serialized as a byte array.
- **Parameters**: Any parameters required by the query, serialized in the correct format.
- **Data**: Additional data needed for the query execution, serialized as needed.

### Constructing the Payload

To construct the payload:

1. **Serialize the Query**: Convert the query string into a byte array using the appropriate character encoding (e.g., UTF-8).
2. **Serialize Parameters**: Convert any parameters into a byte array, ensuring they are in the correct format for KDB+.
3. **Combine Data**: If additional data is needed, serialize it and append it to the payload.

### Payload Serialization

- **Character Encoding**: Use a consistent character encoding (e.g., UTF-8) for serializing strings.
- **Data Types**: Ensure that all data types are correctly serialized according to the KDB+ protocol specifications.

This payload structure is essential for ensuring that the KDB server correctly interprets the incoming data, allowing for efficient and reliable query execution. 

### Byte Buffer Serialization for Each Data Type

1. **Boolean**: 
   - **Buffer Content**: Single byte, `0` for false, `1` for true.

2. **Byte**: 
   - **Buffer Content**: Single byte representing the byte value.

3. **Short**: 
   - **Buffer Content**: 2 bytes, big-endian order.

4. **Int**: 
   - **Buffer Content**: 4 bytes, big-endian order.

5. **Long**: 
   - **Buffer Content**: 8 bytes, big-endian order.

6. **Float**: 
   - **Buffer Content**: 4 bytes, IEEE 754 format, big-endian order.

7. **Double**: 
   - **Buffer Content**: 8 bytes, IEEE 754 format, big-endian order.

8. **Char**: 
   - **Buffer Content**: Single byte representing the character.

9. **String**: 
   - **Buffer Content**: Null-terminated string, encoded in UTF-8.

10. **LocalDate**: 
    - **Buffer Content**: 4 bytes representing days since 2000.01.01, big-endian order.

11. **LocalTime**: 
    - **Buffer Content**: 4 bytes representing milliseconds since midnight, big-endian order.

12. **LocalDateTime**: 
    - **Buffer Content**: 8 bytes representing seconds since 2000.01.01, big-endian order.

13. **List**: 
    - **Buffer Content**: Serialized elements in sequence, prefixed with the list length.

14. **Dictionary**: 
    - **Buffer Content**: Serialized key-value pairs, prefixed with the dictionary size.

15. **Table**: 
    - **Buffer Content**: Serialized columnar data, prefixed with the number of rows and columns.

This detailed serialization guide ensures that each data type is correctly represented in the byte buffer for communication with the KDB server. 

## Supported Protocols for Connecting to KDB

The `c.java` client supports multiple protocols for connecting to the KDB backend, providing flexibility in how connections are established:

1. **TCP (Transmission Control Protocol)**:
   - **Description**: TCP is a standard protocol for network communication, providing reliable, ordered, and error-checked delivery of data.
   - **Usage**: The most common method for connecting to a KDB server, especially over a network.
   - **Example**: `c connection = new c("localhost", 5000);` establishes a TCP connection to the server running on `localhost` at port `5000`.

2. **UDS (Unix Domain Sockets)**:
   - **Description**: UDS is a protocol for inter-process communication on the same host, offering lower latency and higher throughput compared to TCP.
   - **Usage**: Suitable for scenarios where the client and server are on the same machine.
   - **Example**: `c connection = new c("/path/to/socket");` establishes a UDS connection using the specified socket file path.

These protocols allow the `c.java` client to connect to the KDB backend in various environments, ensuring efficient and reliable communication.

## Connecting to the KDB Database

To connect to a KDB database using the `c.java` client, follow these steps:

1. **Import the `c` Class**: Ensure that the `c.java` file is included in your project and import the `c` class.

2. **Establish a Connection**: Create an instance of the `c` class, providing the necessary connection parameters such as the host and port.

3. **Execute Queries**: Use the connection instance to send queries to the KDB server and receive responses.

4. **Close the Connection**: Once the operations are complete, close the connection to free up resources.

#### Example Code

```java
import com.kx.c;

public class KDBClient {
  public static void main(String[] args) {
    try {
      // Establish a connection to the KDB server
      c connection = new c("localhost", 5000);

      // Execute a query
      Object result = connection.k("2+2");

      // Print the result
      System.out.println("Result: " + result);

      // Close the connection
      connection.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
```

#### Connection Parameters

- **Host**: The hostname or IP address of the KDB server.
- **Port**: The port number on which the KDB server is listening.

This guide provides a basic example of how to connect to a KDB database, execute a query, and handle the connection lifecycle. 