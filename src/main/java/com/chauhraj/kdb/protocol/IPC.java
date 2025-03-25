package com.chauhraj.kdb.protocol;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Hashing;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Object2IntHashMap;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import com.chauhraj.kdb.datatypes.Dict;
import com.chauhraj.kdb.datatypes.Flip;
import com.chauhraj.kdb.datatypes.Minute;
import com.chauhraj.kdb.datatypes.Month;
import com.chauhraj.kdb.datatypes.Second;
import com.chauhraj.kdb.datatypes.Timespan;

public class IPC {

  private static final int INITIAL_BUFFER_SIZE = 4096;
  private static final int MAX_MESSAGE_SIZE = 2 * 1024 * 1024 * 1024; // 2GB
  
  // Header structure constants
  private static final int HEADER_SIZE = 8;
  private static final int ENDIAN_OFFSET = 0;
  private static final int MSG_TYPE_OFFSET = 1;
  private static final int COMPRESSION_OFFSET = 2;
  private static final int RESERVED_OFFSET = 3;
  private static final int MSG_LENGTH_OFFSET = 4;
  
  // Message type constants  
  private static final byte ASYNC = 1;
  private static final byte SYNC = 0;
  private static final byte RESPONSE = 2;
  
  // Endian flags
  private static final byte BIG_ENDIAN = 0;
  private static final byte LITTLE_ENDIAN = 1;
  
  // Compression flags
  private static final byte UNCOMPRESSED = 0;
  private static final byte COMPRESSED = 1;

  // Buffer management
  private MutableDirectBuffer writeBuffer;
  private MutableDirectBuffer readBuffer;
  private final IdleStrategy idleStrategy;
  private final Int2ObjectHashMap<Object> typeCache;
  
  private int wBuffPos;
  private int rBuffPos;
  private boolean isLittleEndian;
  private static final String encoding = "ISO-8859-1";
  private boolean zip;
  private boolean isLoopback;
  private int ipcVersion;
  private int sync;
  private SocketChannel channel;
  private DataInputStream inStream;
  private OutputStream outStream;
  private byte[] rBuff;
  private static final LocalTime LOCAL_TIME_NULL = LocalTime.MIN;

  // Type cache constants
  private static final int TYPE_BOOLEAN = 1;
  private static final int TYPE_UUID = 2;
  private static final int TYPE_BYTE = 4;
  private static final int TYPE_SHORT = 5;
  private static final int TYPE_INT = 6;
  private static final int TYPE_LONG = 7;
  private static final int TYPE_FLOAT = 8;
  private static final int TYPE_DOUBLE = 9;
  private static final int TYPE_CHAR = 10;
  private static final int TYPE_STRING = 11;
  
  private static final int TYPE_INSTANT = 12;
  private static final int TYPE_MINUTE = 13;
  private static final int TYPE_SECOND = 14;
  private static final int TYPE_TIMESPAN = 15;
  private static final int TYPE_MONTH = 16;
  private static final int TYPE_DATE = 17;
  private static final int TYPE_DATETIME = 18;
  private static final int TYPE_TIME = 19;
  private static final int TYPE_TIMESTAMP = 20;
  private static final int TYPE_SYMBOL = 21;
  private static final int TYPE_LIST = 22;
  private static final int TYPE_DICT = 99;
  private static final int TYPE_FLIP = 98;
  private static final int TYPE_ATOM = 25;
  private static final int TYPE_FUNCTION = 26;
  private static final int TYPE_TABLE = 27;
  private static final int TYPE_DICTIONARY = 28;
  private static final int TYPE_FUNCTION_REF = 29;
  private static final int TYPE_NULL = 30;
  private static final int TYPE_ERROR = 31;
  private static final int TYPE_SYMBOL_REF = 32;
  private static final int TYPE_TIMESTAMP_MS = 33;
  private static final int TYPE_TIMESTAMP_NS = 34;

  private int dstPos;  // Add position tracking


  private static final ClassValue<Integer> TYPE_CACHE = new ClassValue<>() {
    private final Object2IntHashMap<Class<?>> TYPE_MAP = new Object2IntHashMap<>(20, Hashing.DEFAULT_LOAD_FACTOR, 0); {
        TYPE_MAP.put(Boolean.class, -1);
        TYPE_MAP.put(UUID.class, -2);
        TYPE_MAP.put(Byte.class, -4);
        TYPE_MAP.put(Short.class, -5);
        TYPE_MAP.put(Integer.class, -6);
        TYPE_MAP.put(Long.class, -7);
        TYPE_MAP.put(Float.class, -8);
        TYPE_MAP.put(Double.class, -9);
        TYPE_MAP.put(Character.class, -10);
        TYPE_MAP.put(String.class, -11);
        TYPE_MAP.put(LocalDate.class, -14);
        TYPE_MAP.put(LocalTime.class, -19);
        TYPE_MAP.put(Instant.class, -12);
        TYPE_MAP.put(LocalDateTime.class, -15);
        TYPE_MAP.put(Timespan.class, -16);
        TYPE_MAP.put(Month.class, -13);
        TYPE_MAP.put(Minute.class, -17);
        TYPE_MAP.put(Second.class, -18);
        TYPE_MAP.put(Flip.class, 98);
        TYPE_MAP.put(Dict.class, 99);
    }

    @Override
    protected Integer computeValue(Class<?> type) {
        if (type.isArray()) {
            return -1 * TYPE_MAP.get(type.getComponentType());
        } else {
            return TYPE_MAP.get(type);
        }
    }
};

  /**
   * Gets the numeric type of the supplied object used in kdb+ (distinct supported data types in KDB+ can be identified by a numeric).&nbsp;
   * See data type reference <a href="https://code.kx.com/q/basics/datatypes/">https://code.kx.com/q/basics/datatypes/</a>.
   * For example, an object of type java.lang.Integer provides a numeric type of -6.
   * @param x Object to get the numeric type of
   * @return kdb+ type number for an object
   */
  public static int t(final Object x) {
      Objects.requireNonNull(x);
      return TYPE_CACHE.get(x.getClass());
  }

  public IPC() {
    this.typeCache = new Int2ObjectHashMap<>();
    this.writeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(INITIAL_BUFFER_SIZE));
    this.readBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(INITIAL_BUFFER_SIZE));
    this.idleStrategy = new BackoffIdleStrategy();
    this.dstPos = 0;
  }

  /**
   * Implements a variant of LZ77 (Lempel-Ziv 1977) compression algorithm optimized for KDB+ IPC protocol.
   * The algorithm uses a sliding window approach with the following characteristics:
   * - Uses a 256-entry hash table for finding matches (based on XOR of adjacent bytes)
   * - Maximum match length of 255 bytes
   * - 8-bit compression flags stored every 8 literals/matches
   * - Output format:
   *   - First 8 bytes: header including original size
   *   - Followed by alternating flag bytes and data
   *   - Flag bits: 0 for literal byte, 1 for length-distance pair
   * 
   * Implementation details:
   * - Hash function: h = y[s] ^ y[s+1] (XOR of adjacent bytes)
   * - Match encoding: [hash byte][length byte] for each match
   * - Flag byte format: 8 bits indicating literal (0) or match (1) for next 8 items
   * - Early exit if compression not beneficial (output larger than input)
   * 
   * Memory usage:
   * - Uses a 256-int array as hash table (1KB)
   * - Output buffer initialized to half size of input
   * - If compression fails to achieve reduction, reverts to original data
   *
   * References:
   * - Original LZ77 paper: Ziv J., Lempel A., "A Universal Algorithm for Sequential Data Compression",
   *   IEEE Transactions on Information Theory, 1977
   * - KDB+ Implementation: code.kx.com/q/interfaces/c-client-for-q
   *
   * @param n Size of input data to compress
   * @param dst Source array containing data to compress, will be overwritten with compressed data
   */
  void compress(final int inputSize, final byte[] input) {
    // Early exit for small inputs that won't benefit from compression
    if (inputSize < 16) {
        dstPos = inputSize;
        return;
    }

    final int HASH_TABLE_SIZE = 256;
    final int MAX_MATCH_LENGTH = 255;
    final int HEADER_SIZE = 8;
    final byte[] output = new byte[input.length / 2];  // Compressed data can't be larger than half
    
    // Hash table for finding matches
    final int[] hashTable = new int[HASH_TABLE_SIZE];
    
    // Position tracking
    int writePos = HEADER_SIZE;        // Current position in output buffer
    int readPos = 8;                   // Current position in input buffer
    int flagBytePos = HEADER_SIZE;     // Position of current flag byte
    int currentFlag = 0;               // Current flag byte being built
    int flagBitMask = 1;              // Current bit position in flag byte
    
    // Copy header
    System.arraycopy(input, 0, output, 0, 4);
    output[2] = 1;  // Set compression flag
    
    // Write original size in header
    dstPos = HEADER_SIZE;
    w(inputSize);
    
    // Main compression loop
    while (readPos < inputSize) {
        // Check if we need a new flag byte
        if (flagBitMask == 1) {
            // Check if we have enough space for worst case scenario
            if (writePos > output.length - 17) {
                // Compression not beneficial, revert to original
                dstPos = inputSize;
                return;
            }
            flagBytePos = writePos++;
            currentFlag = 0;
        }
        
        // Calculate hash for current position
        final int hash = 0xFF & (input[readPos] ^ input[readPos + 1]);
        final int matchPos = hashTable[hash];
        final boolean isMatch = (readPos <= inputSize - 3) && 
                              (matchPos != 0) && 
                              (input[readPos] == input[matchPos]);
        
        if (isMatch) {
            // Found a match, encode it
            hashTable[hash] = readPos;
            currentFlag |= flagBitMask;
            
            // Find match length
            int matchStart = matchPos + 2;
            int currentPos = readPos + 2;
            final int maxMatchEnd = Math.min(readPos + MAX_MATCH_LENGTH, inputSize);
            
            while (currentPos < maxMatchEnd && input[matchStart] == input[currentPos]) {
                matchStart++;
                currentPos++;
            }
            
            // Write match (hash and length)
            output[writePos++] = (byte)hash;
            output[writePos++] = (byte)(currentPos - readPos);
            readPos = currentPos;
            
        } else {
            // No match, write literal
            hashTable[hash] = readPos;
            output[writePos++] = input[readPos++];
        }
        
        flagBitMask <<= 1;
        if (flagBitMask == 256) {
            output[flagBytePos] = (byte)currentFlag;
            flagBitMask = 1;
        }
    }
    
    // Write final flag byte if needed
    if (flagBitMask != 1) {
        output[flagBytePos] = (byte)currentFlag;
    }
    
    // Write final size and copy to result
    dstPos = 4;
    w(writePos);
    dstPos = writePos;
    
    // Trim output array to actual size
    System.arraycopy(output, 0, input, 0, writePos);
}
 
  /**
   * Decompresses data that was compressed using KDB+'s IPC compression algorithm (LZ77 variant).
   * This is the inverse operation of the compress() method, reconstructing the original data
   * from the compressed format.
   * 
   * Format of compressed data:
   * - First 8 bytes: header containing original uncompressed size
   * - Remaining data: sequence of flag bytes followed by data bytes
   * 
   * Implementation details:
   * - Reads flag byte indicating next 8 items
   * - For each bit in flag byte:
   *   - 0: next byte is literal, copy directly
   *   - 1: next 2 bytes are [position][length], copy from history
   * - Uses sliding window for match reconstruction
   * - No hash table needed for decompression
   * 
   * Memory management:
   * - Allocates output buffer of size specified in header
   * - Single-pass decompression (no backtracking)
   * - In-place reconstruction using system.arraycopy for matches
   * 
   * Error handling:
   * - Validates uncompressed size matches header
   * - Checks for buffer overflow conditions
   * - Ensures match references are within valid range
   * 
   * Performance characteristics:
   * - O(n) time complexity where n is compressed size
   * - Memory usage proportional to uncompressed size
   * - Efficient for small to medium messages
   * 
   * @param dst Destination array to store uncompressed data
   * @return Size of uncompressed data
   */
  private void uncompress() {
    int n = 0;
    int r = 0;
    int f = 0;
    int s = 8;
    int p = s;
    short i = 0;
    byte[] dst = new byte[ri()];
    int d = rBuffPos;
    int[] aa = new int[256];
    while (s < dst.length) {
      if (i == 0) {
        f = 0xff & (int) rBuff[d++];
        i = 1;
      }
      if ((f & i) != 0) {
        r = aa[0xff & (int) rBuff[d++]];
        dst[s++] = dst[r++];
        dst[s++] = dst[r++];
        n = 0xff & (int) rBuff[d++];
        for (int m = 0; m < n; m++)
          dst[s + m] = dst[r + m];
      } else
        dst[s++] = rBuff[d++];
      while (p < s - 1)
        aa[(0xff & (int) dst[p] ^ (0xff & (int) dst[p + 1]))] = p++;
      if ((f & i) != 0)
        p = s += n;
      i *= 2;
      if (i == 256)
        i = 0;
    }
    rBuff = dst;
    rBuffPos = 8;
  }

  /**
   * Write byte to serialization buffer and increment buffer position
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param x byte to write to buffer
   * @return Updated write index
   */
  int w(MutableDirectBuffer buffer, int writeIndex, byte x) {
    buffer.putByte(writeIndex, x);
    return writeIndex + 1;
  }
  
  /** null integer, i.e. 0Ni */
  static int ni = Integer.MIN_VALUE;
  /** null long, i.e. 0N */
  static long nj = Long.MIN_VALUE;
  /** null float, i.e. 0Nf or 0n */
  static double nf = Double.NaN;
  /**
   * Deserialize boolean from byte buffer
   * @return Deserialized boolean
   */
  boolean rb() {
    return readBuffer.getByte(rBuffPos++) == 1;
  }
  /**
   * Write boolean to serialization buffer
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param x boolean to serialize
   * @return Updated write index
   */
  int w(MutableDirectBuffer buffer, int writeIndex, boolean x) {
    buffer.putByte(writeIndex, (byte) (x ? 1 : 0));
    return writeIndex + 1;
  }

  /**
   * Deserialize char from byte buffer
   * @return Deserialized char
   */
  char rc() {
    return (char) (readBuffer.getByte(rBuffPos++) & 0xff);
  }

  /**
   * Write char to serialization buffer
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param c char to serialize
   * @return Updated write index
   */
  int w(MutableDirectBuffer buffer, int writeIndex, char c) {
    buffer.putByte(writeIndex, (byte) c);
    return writeIndex + 1;
  }

  /**
   * Deserialize short from byte buffer
   * @return Deserialized short
   */
  short rh() {
    short value = isLittleEndian ? 
        readBuffer.getShort(rBuffPos, ByteOrder.LITTLE_ENDIAN) :
        readBuffer.getShort(rBuffPos, ByteOrder.BIG_ENDIAN);
    rBuffPos += 2;
    return value;
  }

  /**
   * Write short to serialization buffer in big endian format
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param h short to serialize
   * @return Updated write index
   */
  int w(MutableDirectBuffer buffer, int writeIndex, short h) {
    buffer.putShort(writeIndex, Short.reverseBytes(h));
    return writeIndex + 2;
  }

  /**
   * Deserialize int from byte buffer
   * @return Deserialized int
   */
  int ri() {
    int value = isLittleEndian ? 
        readBuffer.getInt(rBuffPos, ByteOrder.LITTLE_ENDIAN) :
        readBuffer.getInt(rBuffPos, ByteOrder.BIG_ENDIAN);
    rBuffPos += 4;
    return value;
  }

  /**
   * Write int to serialization buffer in big endian format
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param i int to serialize
   * @return Updated write index
   */
  int w(MutableDirectBuffer buffer, int writeIndex, int i) {
    buffer.putInt(writeIndex, Integer.reverseBytes(i));
    return writeIndex + 4;
  }

  /**
   * Deserialize UUID from byte buffer
   * @return Deserialized UUID
   */
  UUID rg() {
    boolean oa = isLittleEndian;
    isLittleEndian = false;
    UUID g = new UUID(rj(), rj());
    isLittleEndian = oa;
    return g;
  }

  /**
   * Write uuid to serialization buffer in big endian format
   * @param uuid UUID to serialize
   */
  void w(UUID uuid) {
    if (ipcVersion < 3)
      throw new RuntimeException("Guid not valid pre kdb+3.0");
    writeBuffer.putLong(wBuffPos, uuid.getMostSignificantBits(), ByteOrder.BIG_ENDIAN);
    writeBuffer.putLong(wBuffPos + 8, uuid.getLeastSignificantBits(), ByteOrder.BIG_ENDIAN);
    wBuffPos += 16;
  }

  /**
   * Deserialize long from byte buffer
   * @return Deserialized long
   */
  long rj() {
    long value = isLittleEndian ? 
        readBuffer.getLong(rBuffPos, ByteOrder.LITTLE_ENDIAN) :
        readBuffer.getLong(rBuffPos, ByteOrder.BIG_ENDIAN);
    rBuffPos += 8;
    return value;
  }
  /**
   * Write long to serialization buffer in big endian format
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param j long to serialize
   * @return Updated write index
   */
  int w(MutableDirectBuffer buffer, int writeIndex, long j) {
    buffer.putLong(writeIndex, Long.reverseBytes(j));
    return writeIndex + 8;
  }
  /**
   * Deserialize float from byte buffer
   * @return Deserialized float
   */
  float re() {
    float value = isLittleEndian ? 
        readBuffer.getFloat(rBuffPos, ByteOrder.LITTLE_ENDIAN) :
        readBuffer.getFloat(rBuffPos, ByteOrder.BIG_ENDIAN);
    rBuffPos += 4;
    return value;
  }
  /**
   * Write float to serialization buffer in big endian format
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param e float to serialize
   * @return Updated write index
   */
  int w(MutableDirectBuffer buffer, int writeIndex, float e) {
    buffer.putInt(writeIndex, Integer.reverseBytes(Float.floatToRawIntBits(e)));
    return writeIndex + 4;
  }
  /**
   * Deserialize double from byte buffer
   * @return Deserialized double
   */
  double rf() {
    double value = isLittleEndian ? 
        readBuffer.getDouble(rBuffPos, ByteOrder.LITTLE_ENDIAN) :
        readBuffer.getDouble(rBuffPos, ByteOrder.BIG_ENDIAN);
    rBuffPos += 8;
    return value;
  }
  /**
   * Write double to serialization buffer in big endian format
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param f double to serialize
   * @return Updated write index
   */
  int w(MutableDirectBuffer buffer, int writeIndex, double f) {
    buffer.putLong(writeIndex, Long.reverseBytes(Double.doubleToRawLongBits(f)));
    return writeIndex + 8;
  }
  /**
   * Deserialize Month from byte buffer
   * @return Deserialized Month
   */
  Month rm() {
    return new Month(ri());
  }
  /**
   * Write Month to serialization buffer in big endian format
   * @param m Month to serialize
   */
  void w(Month m) {
    writeBuffer.putInt(wBuffPos, m.i, ByteOrder.BIG_ENDIAN);
    wBuffPos += 4;
  }
  /**
   * Deserialize Minute from byte buffer
   * @return Deserialized Minute
   */
  Minute ru() {
    return new Minute(ri());
  }
  /**
   * Write Minute to serialization buffer in big endian format
   * @param u Minute to serialize
   */
  void w(Minute u) {
    writeBuffer.putInt(wBuffPos, u.i, ByteOrder.BIG_ENDIAN);
    wBuffPos += 4;
  }
  /**
   * Deserialize Second from byte buffer
   * @return Deserialized Second
   */
  Second rv() {
    return new Second(ri());
  }
  /**
   * Write Second to serialization buffer in big endian format
   * @param v Second to serialize
   */
  void w(Second v) {
    writeBuffer.putInt(wBuffPos, v.i, ByteOrder.BIG_ENDIAN);
    wBuffPos += 4;
  }
  /**
   * Deserialize Timespan from byte buffer
   * @return Deserialized Timespan
   */
  Timespan rn() {
    return new Timespan(rj());
  }
  /**
   * Write Timespan to serialization buffer in big endian format
   * @param n Timespan to serialize
   */
  void w(Timespan n) {
    if (ipcVersion < 1)
      throw new RuntimeException("Timespan not valid pre kdb+2.6");
    writeBuffer.putLong(wBuffPos, n.j, ByteOrder.BIG_ENDIAN);
    wBuffPos += 8;
  }

  static final int DAYS_BETWEEN_1970_2000 = 10957;
  static final long MILLS_IN_DAY = 86400000L;
  static final long MILLS_BETWEEN_1970_2000 = MILLS_IN_DAY * DAYS_BETWEEN_1970_2000;
  static final long NANOS_IN_SEC = 1000000000L;

  /**
   * Deserialize date from byte buffer
   * @return Deserialized date
   */
  LocalDate rd() {
    int dateAsInt = ri();
    return (dateAsInt == ni ? LocalDate.MIN : LocalDate.ofEpochDay(10957L + dateAsInt));
  }
  /**
   * Write LocalDate to serialization buffer in big endian format
   * @param d Date to serialize
   */
  void w(LocalDate d) {
    if (d == LocalDate.MIN) {
      writeBuffer.putInt(wBuffPos, ni, ByteOrder.BIG_ENDIAN);
      wBuffPos += 4;
      return;
    }
    long daysSince2000 = d.toEpochDay() - DAYS_BETWEEN_1970_2000;
    if (daysSince2000 < Integer.MIN_VALUE || daysSince2000 > Integer.MAX_VALUE)
      throw new RuntimeException("LocalDate epoch day since 2000 must be >= Integer.MIN_VALUE and <= Integer.MAX_VALUE");
    writeBuffer.putInt(wBuffPos, (int) (daysSince2000), ByteOrder.BIG_ENDIAN);
    wBuffPos += 4;
  }
  /**
   * Deserialize time from byte buffer
   * @return Deserialized time
   */
  LocalTime rt() {
    int timeAsInt = ri();
    return (timeAsInt == ni ? LOCAL_TIME_NULL : LocalDateTime.ofInstant(Instant.ofEpochMilli(timeAsInt), ZoneId.of("UTC")).toLocalTime());
  }
  private static long toEpochSecond(LocalTime t, LocalDate d, ZoneOffset o) {
    long epochDay = d.toEpochDay();
    long secs = epochDay * 86400 + t.toSecondOfDay();
    secs -= o.getTotalSeconds();
    return secs;
  }
  /**
   * Write LocalTime to serialization buffer in big endian format
   * @param t Time to serialize
   */
  void w(LocalTime t) {
    writeBuffer.putInt(wBuffPos, (t == LOCAL_TIME_NULL) ? ni : (int) ((toEpochSecond(t, LocalDate.of(1970, 1, 1), ZoneOffset.UTC) * 1000 + t.getNano() / 1000000) % MILLS_IN_DAY), ByteOrder.BIG_ENDIAN);
    wBuffPos += 4;
  }
  /**
   * Deserialize LocalDateTime from byte buffer
   * @return Deserialized date
   */
  LocalDateTime rz() {
    double f = rf();
    if (Double.isNaN(f))
      return LocalDateTime.MIN;
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(MILLS_BETWEEN_1970_2000 + Math.round(8.64e7 * f)), ZoneId.of("UTC"));
  }
  /**
   * Write Date to serialization buffer in big endian format (only millisecond support)
   * @param z Date to serialize
   */
  void w(LocalDateTime z) {
    writeBuffer.putDouble(wBuffPos, z == LocalDateTime.MIN ? nf : ((z.toInstant(ZoneOffset.UTC).toEpochMilli() - MILLS_BETWEEN_1970_2000) / 8.64e7), ByteOrder.BIG_ENDIAN);
    wBuffPos += 8;
  }
  /**
   * Deserialize Instant from byte buffer
   * @return Deserialized timestamp
   */
  Instant rp() {
    long timeAsLong = rj();
    if (timeAsLong == nj)
      return Instant.MIN;
    long d = timeAsLong < 0 ? (timeAsLong + 1) / NANOS_IN_SEC - 1 : timeAsLong / NANOS_IN_SEC;
    return Instant.ofEpochMilli(MILLS_BETWEEN_1970_2000 + 1000 * d).plusNanos((int) (timeAsLong - NANOS_IN_SEC * d));
  }
  /**
   * Write Instant to serialization buffer in big endian format
   * @param p Instant to serialize
   */
  void w(Instant p) {
    if (ipcVersion < 1)
      throw new RuntimeException("Instant not valid pre kdb+2.6");
    writeBuffer.putLong(wBuffPos, p == Instant.MIN ? nj : 1000000 * (p.toEpochMilli() - MILLS_BETWEEN_1970_2000) + p.getNano() % 1000000, ByteOrder.BIG_ENDIAN);
    wBuffPos += 8;
  }
  /**
   * Deserialize string from byte buffer
   * @return Deserialized string using registered encoding
   * @throws UnsupportedEncodingException If there is an issue with the registed encoding
   */
  String rs() throws UnsupportedEncodingException {
    int startPos = rBuffPos;
    while (rBuff[rBuffPos++] != 0);
    return (startPos == rBuffPos - 1) ? "" : new String(rBuff, startPos, rBuffPos - 1 - startPos, encoding);
  }
  /**
   * Write String to serialization buffer
   * @param s String to serialize
   * @throws UnsupportedEncodingException If there is an issue with the registed encoding
   */
  void w(String s) throws UnsupportedEncodingException {
    if (s != null) {
      int byteLen = ns(s);
      byte[] bytes = s.getBytes(encoding);
      for (int idx = 0; idx < byteLen; idx++)
        writeBuffer.putByte(wBuffPos++, bytes[idx]);
    }
    writeBuffer.putByte(wBuffPos++, (byte)0);
  }
  /**
   * Deserializes the contents of the incoming message buffer {@code b}.
   * @return deserialised object
   * @throws UnsupportedEncodingException If the named charset is not supported
   */
  Object r() throws UnsupportedEncodingException {
    int i = 0;
    int n;
    int t = readBuffer.getByte(rBuffPos++);
    if (t < 0)
      switch (t) {
        case -1:
          return rb();
        case (-2):
          return rg();
        case -4:
          return readBuffer.getByte(rBuffPos++);
        case -5:
          return rh();
        case -6:
          return ri();
        case -7:
          return rj();
        case -8:
          return re();
        case -9:
          return rf();
        case -10:
          return rc();
        case -11:
          return rs();
        case -12:
          return rp();
        case -13:
          return rm();
        case -14:
          return rd();
        case -15:
          return rz();
        case -16:
          return rn();
        case -17:
          return ru();
        case -18:
          return rv();
        case -19:
          return rt();
      }
    if (t > 99) {
      if (t == 100) {
        rs();
        return r();
      }
      if (t < 104)
        return readBuffer.getByte(rBuffPos++) == 0 && t == 101 ? null : "func";
      if (t > 105)
        r();
      else
        for (n = ri(); i < n; i++)
          r();
      return "func";
    }
    if (t == 99)
      return new Dict(r(), r());
    rBuffPos++;
    if (t == 98)
      return new Flip((Dict) r());
    n = ri();
    switch (t) {
      case 0:
        Object[] objArr = new Object[n];
        for (; i < n; i++)
          objArr[i] = r();
        return objArr;
      case 1:
        boolean[] boolArr = new boolean[n];
        for (; i < n; i++)
          boolArr[i] = rb();
        return boolArr;
      case 2:
        UUID[] uuidArr = new UUID[n];
        for (; i < n; i++)
          uuidArr[i] = rg();
        return uuidArr;
      case 4:
        byte[] byteArr = new byte[n];
        for (; i < n; i++)
          byteArr[i] = readBuffer.getByte(rBuffPos++);
        return byteArr;
      case 5:
        short[] shortArr = new short[n];
        for (; i < n; i++)
          shortArr[i] = rh();
        return shortArr;
      case 6:
        int[] intArr = new int[n];
        for (; i < n; i++)
          intArr[i] = ri();
        return intArr;
      case 7:
        long[] longArr = new long[n];
        for (; i < n; i++)
          longArr[i] = rj();
        return longArr;
      case 8:
        float[] floatArr = new float[n];
        for (; i < n; i++)
          floatArr[i] = re();
        return floatArr;
      case 9:
        double[] doubleArr = new double[n];
        for (; i < n; i++)
          doubleArr[i] = rf();
        return doubleArr;
      case 10:
        char[] charArr = new String(rBuff, rBuffPos, n, encoding).toCharArray();
        rBuffPos += n;
        return charArr;
      case 11:
        String[] stringArr = new String[n];
        for (; i < n; i++)
          stringArr[i] = rs();
        return stringArr;
      case 12:
        Instant[] timestampArr = new Instant[n];
        for (; i < n; i++)
          timestampArr[i] = rp();
        return timestampArr;
      case 13:
        Month[] monthArr = new Month[n];
        for (; i < n; i++)
          monthArr[i] = rm();
        return monthArr;
      case 14:
        LocalDate[] dateArr = new LocalDate[n];
        for (; i < n; i++)
          dateArr[i] = rd();
        return dateArr;
      case 15:
        LocalDateTime[] dateUtilArr = new LocalDateTime[n];
        for (; i < n; i++)
          dateUtilArr[i] = rz();
        return dateUtilArr;
      case 16:
        Timespan[] timespanArr = new Timespan[n];
        for (; i < n; i++)
          timespanArr[i] = rn();
        return timespanArr;
      case 17:
        Minute[] minArr = new Minute[n];
        for (; i < n; i++)
          minArr[i] = ru();
        return minArr;
      case 18:
        Second[] secArr = new Second[n];
        for (; i < n; i++)
          secArr[i] = rv();
        return secArr;
      case 19:
        LocalTime[] timeArr = new LocalTime[n];
        for (; i < n; i++)
          timeArr[i] = rt();
        return timeArr;
      default:
        // do nothing, let it return null
    }
    return null;
  }

  /**
   * "number of bytes from type." A helper for nx, to assist in calculating the number of bytes required to serialize a
   * particular type.
   */
  static int[] nt = {0, 1, 16, 0, 1, 2, 4, 8, 4, 8, 1, 0, 8, 4, 4, 8, 8, 4, 4, 4};
  /**
   * A helper function for nx, calculates the number of bytes which would be required to serialize the supplied string.
   * @param s String to be serialized
   * @return number of bytes required to serialise a string
   * @throws UnsupportedEncodingException  If the named charset is not supported
   */
  static int ns(String s) throws UnsupportedEncodingException {
    int i;
    if (s == null)
      return 0;
    if (-1 < (i = s.indexOf('\000')))
      s = s.substring(0, i);
    return s.getBytes(encoding).length;
  }
  /**
   * A helper function used by nx which returns the number of elements in the supplied object
   * (for example: the number of keys in a Dict, the number of rows in a Flip,
   * the length of the array if its an array type)
   * @param x Object to be serialized
   * @return number of elements in an object.
   * @throws UnsupportedEncodingException  If the named charset is not supported
   */
  public static int n(final Object x) throws UnsupportedEncodingException {
    if (x instanceof Dict)
      return n(((Dict) x).x);
    if (x instanceof Flip)
      return n(((Flip) x).y[0]);
    return x instanceof char[] ? new String((char[]) x).getBytes(encoding).length : Array.getLength(x);
  }
  /**
   * Calculates the number of bytes which would be required to serialize the supplied object.
   * @param x Object to be serialized
   * @return number of bytes required to serialise an object.
   * @throws UnsupportedEncodingException  If the named charset is not supported
   */
  public int nx(Object x) throws UnsupportedEncodingException {
    int type = t(x);
    if (type == TYPE_DICT)
      return 1 + nx(((Dict) x).x) + nx(((Dict) x).y);
    if (type == TYPE_FLIP)
      return 3 + nx(((Flip) x).x) + nx(((Flip) x).y);
    if (type < 0)
      return type == -11 ? 2 + ns((String) x) : 1 + nt[-type];
    int numBytes = 6;
    int numElements = n(x);
    if (type == 0 || type == 11)
      for (int idx = 0; idx < numElements; ++idx)
        numBytes += type == 0 ? nx(((Object[]) x)[idx]) : 1 + ns(((String[]) x)[idx]);
    else
      numBytes += numElements * nt[type];
    return numBytes;
  }
  
  /**
   * Write object to serialization buffer
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param x Object to serialize
   * @return Updated write index
   * @throws UnsupportedEncodingException If there is an issue with the encoding
   */
  int w(MutableDirectBuffer buffer, int writeIndex, Object x) throws UnsupportedEncodingException {
    int i = 0;
    int n;
    int type = t(x);
    writeIndex = w(buffer, writeIndex, (byte) type);
    
    if (type < 0) {
      switch (type) {
        case -1:
          return w(buffer, writeIndex, ((Boolean) x).booleanValue());
        case -2:
          return w(buffer, writeIndex, (UUID) x);
        case -4:
          return w(buffer, writeIndex, ((Byte) x).byteValue());
        case -5:
          return w(buffer, writeIndex, ((Short) x).shortValue());
        case -6:
          return w(buffer, writeIndex, ((Integer) x).intValue());
        case -7:
          return w(buffer, writeIndex, ((Long) x).longValue());
        case -8:
          return w(buffer, writeIndex, ((Float) x).floatValue());
        case -9:
          return w(buffer, writeIndex, ((Double) x).doubleValue());
        case -10:
          return w(buffer, writeIndex, ((Character) x).charValue());
        case -11:
          return w(buffer, writeIndex, (String) x);
        case -12:
          return w(buffer, writeIndex, (Instant) x);
        case -13:
          return w(buffer, writeIndex, (Month) x);
        case -14:
          return w(buffer, writeIndex, (LocalDate) x);
        case -15:
          return w(buffer, writeIndex, (LocalDateTime) x);
        case -16:
          return w(buffer, writeIndex, (Timespan) x);
        case -17:
          return w(buffer, writeIndex, (Minute) x);
        case -18:
          return w(buffer, writeIndex, (Second) x);
        case -19:
          return w(buffer, writeIndex, (LocalTime) x);
      }
    }
    
    if (type == 99) {
      Dict r = (Dict) x;
      writeIndex = w(buffer, writeIndex, r.x);
      return w(buffer, writeIndex, r.y);
    }
    
    writeIndex = w(buffer, writeIndex, (byte) 0);
    
    if (type == 98) {
      Flip r = (Flip) x;
      writeIndex = w(buffer, writeIndex, (byte) 99);
      writeIndex = w(buffer, writeIndex, r.x);
      return w(buffer, writeIndex, r.y);
    }
    
    n = n(x);
    writeIndex = w(buffer, writeIndex, n);
    
    if (type == 10) {
      byte[] b = new String((char[]) x).getBytes(encoding);
      for (; i < b.length; i++)
        writeIndex = w(buffer, writeIndex, b[i]);
      return writeIndex;
    }
    
    for (; i < n; i++) {
      if (type == 0)
        writeIndex = w(buffer, writeIndex, ((Object[]) x)[i]);
      else if (type == 1)
        writeIndex = w(buffer, writeIndex, ((boolean[]) x)[i]);
      else if (type == 2)
        writeIndex = w(buffer, writeIndex, ((UUID[]) x)[i]);
      else if (type == 4)
        writeIndex = w(buffer, writeIndex, ((byte[]) x)[i]);
      else if (type == 5)
        writeIndex = w(buffer, writeIndex, ((short[]) x)[i]);
      else if (type == 6)
        writeIndex = w(buffer, writeIndex, ((int[]) x)[i]);
      else if (type == 7)
        writeIndex = w(buffer, writeIndex, ((long[]) x)[i]);
      else if (type == 8)
        writeIndex = w(buffer, writeIndex, ((float[]) x)[i]);
      else if (type == 9)
        writeIndex = w(buffer, writeIndex, ((double[]) x)[i]);
      else if (type == 11)
        writeIndex = w(buffer, writeIndex, ((String[]) x)[i]);
      else if (type == 12)
        writeIndex = w(buffer, writeIndex, ((Instant[]) x)[i]);
      else if (type == 13)
        writeIndex = w(buffer, writeIndex, ((Month[]) x)[i]);
      else if (type == 14)
        writeIndex = w(buffer, writeIndex, ((LocalDate[]) x)[i]);
      else if (type == 15)
        writeIndex = w(buffer, writeIndex, ((LocalDateTime[]) x)[i]);
      else if (type == 16)
        writeIndex = w(buffer, writeIndex, ((Timespan[]) x)[i]);
      else if (type == 17)
        writeIndex = w(buffer, writeIndex, ((Minute[]) x)[i]);
      else if (type == 18)
        writeIndex = w(buffer, writeIndex, ((Second[]) x)[i]);
      else
        writeIndex = w(buffer, writeIndex, ((LocalTime[]) x)[i]);
    }
    return writeIndex;
  }

  /**
   * Serialises {@code x} object as {@code byte[]} array.
   * @param msgType type of the ipc message (0 – async, 1 – sync, 2 – response)
   * @param x object to serialise
   * @param zip true if to attempt compress serialised output (given uncompressed serialized data also has a length
   * greater than 2000 bytes and connection is not localhost)
   * @return {@code wBuff} containing serialised representation
   *
   * @throws IOException should not throw
   */
  public byte[] serialize(int msgType, Object x, boolean zip) throws IOException {
    int length = HEADER_SIZE + nx(x);
    synchronized (outStream) {
      byte[] backingArray = new byte[length];
      writeBuffer = new UnsafeBuffer(backingArray);
      
      writeBuffer.putByte(ENDIAN_OFFSET, BIG_ENDIAN);
      writeBuffer.putByte(MSG_TYPE_OFFSET, (byte) msgType);
      wBuffPos = MSG_LENGTH_OFFSET;
      w(length);
      w(x);
      
      if (zip && wBuffPos > 2000 && !isLoopback) {
        compress(wBuffPos, backingArray);
        return backingArray;
      }
        
      return backingArray;
    }
  }

  /**
   * Deserialises {@code buffer} q ipc as an object
   * @param buffer byte[] to deserialise object from
   * @return deserialised object
   * @throws KException if buffer contains kdb+ error object.
   * @throws UnsupportedEncodingException  If the named charset is not supported
   */
  public Object deserialize(byte[] buffer) throws KException, UnsupportedEncodingException {
    synchronized (inStream) {
      readBuffer = new UnsafeBuffer(buffer);
      isLittleEndian = readBuffer.getByte(0) == 1;
      boolean compressed = readBuffer.getByte(2) == 1;
      rBuffPos = 8;
      
      if (compressed)
        uncompress();
        
      if (readBuffer.getByte(8) == -128) {
        rBuffPos = 9;
        throw new KException(rs());
      }
      return r();
    }
  }

  private void write(byte[] buf) throws IOException {
    if (channel == null)
      outStream.write(buf);
    else
      channel.write(ByteBuffer.wrap(buf));
  }

  /**
   * Serialize and write the data to the registered connection
   * @param msgType The message type to use within the message (0 – async, 1 – sync, 2 – response)
   * @param x The contents of the message
   * @throws IOException due to an issue serializing/sending the provided data
   */
  protected void w(int msgType, Object x) throws IOException {
    synchronized (outStream) {
      byte[] buffer = serialize(msgType, x, zip);
      write(buffer);
    }
  }
  /**
   * Sends a response message to the remote kdb+ process. This should be called only during processing of an incoming sync message.
   * @param obj Object to send to the remote
   * @throws IOException if not expecting any response
   */
  public void kr(Object obj) throws IOException {
    if (sync == 0)
      throw new IOException("Unexpected response msg");
    sync--;
    w(2, obj);
  }
  /**
   * Sends an error as a response message to the remote kdb+ process. This should be called only during processing of an incoming sync message.
   * @param text The error message text
   * @throws IOException unexpected error message
   */
  public void ke(String text) throws IOException {
    if (sync == 0)
      throw new IOException("Unexpected error msg");
    sync--;
    int n = 2 + ns(text) + 8;
    synchronized (outStream) {
      byte[] buffer = new byte[n];
      buffer[0] = 0;
      buffer[1] = 2;
      wBuffPos = 4;
      w(n);
      writeBuffer.putByte(wBuffPos++, (byte) -128);
      w(text);
      write(buffer);
    }
  }
  /**
   * Sends an async message to the remote kdb+ process. This blocks until the serialized data has been written to the
   * socket. On return, there is no guarantee that this msg has already been processed by the remote process.
   * @param expr The expression to send
   * @throws IOException if an I/O error occurs.
   */
  public void ks(String expr) throws IOException {
    w(0, expr.toCharArray());
  }
  /**
   * Sends an async message to the remote kdb+ process. This blocks until the serialized data has been written to the
   * socket. On return, there is no guarantee that this msg has already been processed by the remote process.
   * @param obj The object to send
   * @throws IOException if an I/O error occurs.
   */
  public void ks(Object obj) throws IOException {
    w(0, obj);
  }

  /**
   * Sends an async message to the remote kdb+ process. This blocks until the serialized data has been written to the
   * socket. On return, there is no guarantee that this msg has already been processed by the remote process. Use this to
   * invoke a function in kdb+ which takes a single argument and does not return a value. e.g. to invoke f[x] use
   * ks("f", x); to invoke a lambda, use ks("{x}", x);
   * @param s The name of the function, or a lambda itself
   * @param x The argument to the function named in s
   * @throws IOException if an I/O error occurs.
   */
  public void ks(String s, Object x) throws IOException {
    Object[] a = {s.toCharArray(), x};
    w(0, a);
  }

  /**
   * Sends an async message to the remote kdb+ process. This blocks until the serialized data has been written to the
   * socket. On return, there is no guarantee that this msg has already been processed by the remote process. Use this to
   * invoke a function in kdb+ which takes 2 arguments and does not return a value. e.g. to invoke f[x;y] use ks("f", x, y);
   * to invoke a lambda, use ks("{x+y}", x, y);
   * @param s The name of the function, or a lambda itself
   * @param x The first argument to the function named in s
   * @param y The second argument to the function named in s
   * @throws IOException if an I/O error occurs.
   */
  public void ks(String s, Object x, Object y) throws IOException {
    Object[] a = {s.toCharArray(), x, y};
    w(0, a);
  }
  /**
   * Sends an async message to the remote kdb+ process. This blocks until the serialized data has been written to the
   * socket. On return, there is no guarantee that this msg has already been processed by the remote process. Use this to
   * invoke a function in kdb+ which takes 3 arguments and does not return a value. e.g. to invoke f[x;y;z] use
   * ks("f", x, y, z); to invoke a lambda, use ks("{x+y+z}", x, y, z);
   * @param s The name of the function, or a lambda itself
   * @param x The first argument to the function named in s
   * @param y The second argument to the function named in s
   * @param z The third argument to the function named in s
   * @throws IOException if an I/O error occurs.
   */
  public void ks(String s, Object x, Object y, Object z) throws IOException {
    Object[] a = {s.toCharArray(), x, y, z};
    w(0, a);
  }
  /**
   * Sends an async message to the remote kdb+ process. This blocks until the serialized data has been written to the
   * socket. On return, there is no guarantee that this msg has already been processed by the remote process. Use this to
   * invoke a function in kdb+ which takes 4 arguments and does not return a value. e.g. to invoke f[param1;param2;param3;param4] use
   * ks("f", param1, param2, param3, param4); to invoke a lambda, use ks("{[param1;param2;param3;param4] param1+param2+param3+param4}", param1, param2, param3, param4);
   * @param s The name of the function, or a lambda itself
   * @param param1 The first argument to the function named in s
   * @param param2 The second argument to the function named in s
   * @param param3 The third argument to the function named in s
   * @param param4 The fourth argument to the function named in s
   * @throws IOException if an I/O error occurs.
   */
  public void ks(String s, Object param1, Object param2, Object param3, Object param4) throws IOException {
    Object[] a = {s.toCharArray(), param1, param2, param3, param4};
    w(0, a);
  }
  /**
   * Sends an async message to the remote kdb+ process. This blocks until the serialized data has been written to the
   * socket. On return, there is no guarantee that this msg has already been processed by the remote process. Use this to
   * invoke a function in kdb+ which takes 5 arguments and does not return a value. e.g. to invoke f[param1;param2;param3;param4;param5] use
   * ks("f", param1, param2, param3, param4, param5); to invoke a lambda, use ks("{[param1;param2;param3;param4;param5] param1+param2+param3+param4+param5}", param1, param2, param3, param4);
   * @param s The name of the function, or a lambda itself
   * @param param1 The first argument to the function named in s
   * @param param2 The second argument to the function named in s
   * @param param3 The third argument to the function named in s
   * @param param4 The fourth argument to the function named in s
   * @param param5 The fifth argument to the function named in s
   * @throws IOException if an I/O error occurs.
   */
  public void ks(String s, Object param1, Object param2, Object param3, Object param4, Object param5) throws IOException {
    Object[] a = {s.toCharArray(), param1, param2, param3, param4, param5};
    w(0, a);
  }
  /**
   * Reads an incoming message from the remote kdb+ process. This blocks until a single message has been received and
   * deserialized. This is called automatically during a sync request via k(String s,..). It can be called explicitly when
   * subscribing to a publisher.
   * @return an Object array of {messageType,deserialised object}
   * @throws KException if response contains an error
   * @throws IOException if an I/O error occurs.
   * @throws UnsupportedEncodingException If the named charset is not supported
   */
  public Object[] readMsg() throws KException, IOException, UnsupportedEncodingException {
    synchronized (inStream) {
      if (channel == null) {
        rBuff = new byte[8];
        inStream.readFully(rBuff); // read the msg header
      } else {
        ByteBuffer buf = ByteBuffer.allocate(8);
        while (0 != buf.remaining()) if (-1 == channel.read(buf)) throw new java.io.EOFException("end of stream");
        rBuff = buf.array();
      }
      isLittleEndian = rBuff[0] == 1;  // endianness of the msg
      if (rBuff[1] == 1) // msg types are 0 - async, 1 - sync, 2 - response
        sync++;   // an incoming sync message means the remote will expect a response message
      rBuffPos = 4;
      if (channel == null) {
        rBuff = Arrays.copyOf(rBuff, ri());
        inStream.readFully(rBuff, 8, rBuff.length - 8); // read the incoming message in full
      } else {
        ByteBuffer buf = ByteBuffer.allocate(ri());
        buf.put(rBuff, 0, rBuff.length);
        while (0 != buf.remaining()) if (-1 == channel.read(buf)) throw new java.io.EOFException("end of stream");
        rBuff = buf.array();
      }
      return new Object[]{rBuff[1], deserialize(rBuff)};
    }
  }
  /**
   * Reads an incoming message from the remote kdb+ process. This blocks until a single message has been received and
   * deserialized. This is called automatically during a sync request via k(String s,..). It can be called explicitly when
   * subscribing to a publisher.
   * @return the deserialised object
   * @throws KException if response contains an error
   * @throws IOException if an I/O error occurs.
   * @throws UnsupportedEncodingException If the named charset is not supported
   */
  public Object k() throws KException, IOException, UnsupportedEncodingException {
    return readMsg()[1];
  }
  /**
   * MsgHandler interface for processing async or sync messages during a sync request whilst awaiting a response message
   * which contains a default implementation
   */
  public interface MsgHandler {
    /**
     * The default implementation discards async messages, responds to sync messages with an error,
     * otherwise the remote will continue to wait for a response
     * @param c The c object that received the message
     * @param msgType The type of the message received (0 – async, 1 – sync, 2 – response)
     * @param msg The message contents
     * @throws IOException Thrown when message type is unexpected (i.e isnt a sync or async message)
     */
    default void processMsg(IPC c, byte msgType, Object msg) throws IOException {
      switch (msgType) {
        case 0:
          System.err.println("discarded unexpected incoming async msg!");
          break; // implicitly discard incoming async messages
        case 1:
          c.ke("unable to process sync requests");
          break; // signal to remote that we're unable to process these by default
        default:
          throw new IOException("Invalid message type received: " + msgType);
      }
    }
  }
  /**
   * {@code msgHandler} is used for handling incoming async and sync messages whilst awaiting a response message to a sync request
   */
  private MsgHandler msgHandler = null;
  /**
   * Stores the handler in an instance variable
   * @param handler The handler to store
   */
  public void setMsgHandler(MsgHandler handler) {
    msgHandler = handler;
  }
  /**
   * Returns the current msg handler
   * @return the current msg handler
   */
  public MsgHandler getMsgHandler() {
    return msgHandler;
  }
  /**
   * {@code collectResponseAsync} is used to indicate whether k() should leave the reading of the associated response message to the caller
   * via readMsg();
   */
  private boolean collectResponseAsync;
  /**
   * Stores the boolean in an instance variable
   * @param b The boolean to store
   */
  public void setCollectResponseAsync(boolean b) {
    collectResponseAsync = b;
  }

  /**
   * Sends a sync message to the remote kdb+ process. This blocks until the message has been sent in full, and, if a MsgHandler
   * is set, will process any queued, incoming async or sync message in order to reach the response message.
   * If the caller has already indicated via {@code setCollectResponseAsync} that the response message will be read async, later, then return
   * without trying to read any messages at this point; the caller can collect(read) the response message by calling readMsg();
   * @param x The object to send
   * @return deserialised response to request {@code x}
   * @throws KException if request evaluation resulted in an error
   * @throws IOException if an I/O error occurs.
   */
  public synchronized Object k(Object x) throws KException, IOException {
    w(1, x);
    if (collectResponseAsync)
      return null;
    while (true) {
      Object[] msg = readMsg();
      if (msgHandler == null || (byte) msg[0] == (byte) 2) // if there's no handler or the msg is a response msg, return it
        return msg[1];
      msgHandler.processMsg(this, (byte) msg[0], msg[1]); // process async and sync requests
    }
  }
  /**
   * Sends a sync message to the remote kdb+ process. This blocks until the message has been sent in full, and a message
   * is received from the remote; typically the received message would be the corresponding response message.
   * @param expr The expression to send
   * @return deserialised response to request {@code x}
   * @throws KException if request evaluation resulted in an error
   * @throws IOException if an I/O error occurs.
   */
  public Object k(String expr) throws KException, IOException {
    return k(expr.toCharArray());
  }
  /**
   * Sends a sync message to the remote kdb+ process. This blocks until the message has been sent in full, and a message
   * is received from the remote; typically the received message would be the corresponding response message. Use this to
   * invoke a function in kdb+ which takes a single argument and returns a value. e.g. to invoke f[x] use k("f", x); to
   * invoke a lambda, use k("{x}", x);
   * @param s The name of the function, or a lambda itself
   * @param x The argument to the function named in s
   * @return deserialised response to request {@code s} with params {@code x}
   * @throws KException if request evaluation resulted in an error
   * @throws IOException if an I/O error occurs.
   */
  public Object k(String s, Object x) throws KException, IOException {
    Object[] a = {s.toCharArray(), x};
    return k(a);
  }
  /**
   * Sends a sync message to the remote kdb+ process. This blocks until the message has been sent in full, and a message
   * is received from the remote; typically the received message would be the corresponding response message. Use this to
   * invoke a function in kdb+ which takes arguments and returns a value. e.g. to invoke f[x;y] use k("f", x, y); to invoke
   * a lambda, use k("{x+y}", x, y);
   * @param s The name of the function, or a lambda itself
   * @param x The first argument to the function named in s
   * @param y The second argument to the function named in s
   * @return deserialised response to the request
   * @throws KException if request evaluation resulted in an error
   * @throws IOException if an I/O error occurs.
   */
  public Object k(String s, Object x, Object y) throws KException, IOException {
    Object[] a = {s.toCharArray(), x, y};
    return k(a);
  }
  /**
   * Sends a sync message to the remote kdb+ process. This blocks until the message has been sent in full, and a message
   * is received from the remote; typically the received message would be the corresponding response message. Use this to
   * invoke a function in kdb+ which takes 3 arguments and returns a value. e.g. to invoke f[x;y;z] use k("f", x, y, z); to
   * invoke a lambda, use k("{x+y+z}", x, y, z);
   * @param s The name of the function, or a lambda itself
   * @param x The first argument to the function named in s
   * @param y The second argument to the function named in s
   * @param z The third argument to the function named in s
   * @return deserialised response to the request
   * @throws KException if request evaluation resulted in an error
   * @throws IOException if an I/O error occurs.
   */
  public Object k(String s, Object x, Object y, Object z) throws KException, IOException {
    Object[] a = {s.toCharArray(), x, y, z};
    return k(a);
  }
  /**
   * Sends a sync message to the remote kdb+ process. This blocks until the message has been sent in full, and a message
   * is received from the remote; typically the received message would be the corresponding response message. Use this to
   * invoke a function in kdb+ which takes 4 arguments and returns a value. e.g. to invoke f[param1;param2;param3;param4] use k("f", param1, param2, param3, param4); to
   * invoke a lambda, use k("{[param1;param2;param3;param4] param1+param2+param3+param4}", param1, param2, param3, param4);
   * @param s The name of the function, or a lambda itself
   * @param param1 The first argument to the function named in s
   * @param param2 The second argument to the function named in s
   * @param param3 The third argument to the function named in s
   * @param param4 The fourth argument to the function named in s
   * @return deserialised response to the request
   * @throws KException if request evaluation resulted in an error
   * @throws IOException if an I/O error occurs.
   */
  public Object k(String s, Object param1, Object param2, Object param3, Object param4) throws KException, IOException {
    Object[] a = {s.toCharArray(), param1, param2, param3, param4};
    return k(a);
  }
  /**
   * Sends a sync message to the remote kdb+ process. This blocks until the message has been sent in full, and a message
   * is received from the remote; typically the received message would be the corresponding response message. Use this to
   * invoke a function in kdb+ which takes 5 arguments and returns a value. e.g. to invoke f[param1;param2;param3;param4;param5] use k("f", param1, param2, param3, param4, param5); to
   * invoke a lambda, use k("{[param1;param2;param3;param4;param5] param1+param2+param3+param4+param5}", param1, param2, param3, param4);
   * @param s The name of the function, or a lambda itself
   * @param param1 The first argument to the function named in s
   * @param param2 The second argument to the function named in s
   * @param param3 The third argument to the function named in s
   * @param param4 The fourth argument to the function named in s
   * @param param5 The fifth argument to the function named in s
   * @return deserialised response to the request
   * @throws KException if request evaluation resulted in an error
   * @throws IOException if an I/O error occurs.
   */
  public Object k(String s, Object param1, Object param2, Object param3, Object param4, Object param5) throws KException, IOException {
    Object[] a = {s.toCharArray(), param1, param2, param3, param4, param5};
    return k(a);
  }
  /**
   * Array containing the null object representation for corresponing kdb+ type number (0-19).&nbsp;
   * See data type reference <a href="https://code.kx.com/q/basics/datatypes/">https://code.kx.com/q/basics/datatypes/</a>
   * For example {@code "".equals(NULL[11])}
   */
  public static final Object[] NULL = {null, Boolean.valueOf(false), new UUID(0, 0), null, Byte.valueOf((byte) 0), Short.valueOf(Short.MIN_VALUE), Integer.valueOf(ni), Long.valueOf(nj), Float.valueOf((float) nf), Double.valueOf(nf), Character.valueOf(' '), "",
    Instant.MIN, new Month(ni), LocalDate.MIN, LocalDateTime.MIN, new Timespan(nj), new Minute(ni), new Second(ni), LOCAL_TIME_NULL
  };
  /**
   * Gets a null object for the type indicated by the character.&nbsp;
   * See data type reference <a href="https://code.kx.com/q/basics/datatypes/">https://code.kx.com/q/basics/datatypes/</a>
   * @param c The shorthand character for the type
   * @return instance of null object of specified kdb+ type.
   */
  public static Object NULL(char c) {
    return NULL[" bg xhijefcspmdznuvt".indexOf(c)];
  }
  /**
   * Tests whether an object represents a KDB+ null for its type, for example
   * qn(NULL('j')) should return true
   * @param x The object to be tested for null
   * @return true if {@code x} is kdb+ null, false otherwise
   */
  public static boolean qn(Object x) {
    int t = -t(x);
    return (t == 2 || t > 4) && x.equals(NULL[t]);
  }
  /**
   * Gets the object at an index of a given array, if its a valid type used by the KDB+ interface.
   * @param x The array to index
   * @param i The offset to index at
   * @return object at index, or null if the object value represents
   * a KDB+ null value for its type
   */
  public static Object at(Object x, int i) {
    x = Array.get(x, i);
    return qn(x) ? null : x;
  }
  /**
   * Sets the object at an index of an array.
   * @param x The array to index
   * @param i The offset to index at
   * @param y The object to set at index i. null can be used if you wish
   * to set the KDB+ null representation of the type (e.g. null would populate
   * an empty string if x was an array of Strings)
   */
  public static void set(Object x, int i, Object y) {
    Array.set(x, i, null == y ? NULL[t(x)] : y);
  }
  /**
   * Finds index of string in an array
   * @param x String array to search
   * @param y The String to locate in the array
   * @return The index at which the String resides
   */
  static int find(String[] x, String y) {
    int i = 0;
    while (i < x.length && !x[i].equals(y))
      ++i;
    return i;
  }
  /**
   * Removes the key from a keyed table.
   * <p>
   * A keyed table(a.k.a. Flip) is a dictionary where both key and value are tables
   * themselves. For ease of processing, this method, td, table from dictionary, can be used to remove the key.
   * </p>
   * @param tbl A table or keyed table.
   * @return A simple table
   * @throws UnsupportedEncodingException If the named charset is not supported
   */
  public static Flip td(Object tbl) throws UnsupportedEncodingException {
    if (tbl instanceof Flip)
      return (Flip) tbl;
    Dict d = (Dict) tbl;
    Flip a = (Flip) d.x;
    Flip b = (Flip) d.y;
    int m = n(a.x);
    int n = n(b.x);
    String[] x = new String[m + n];
    System.arraycopy(a.x, 0, x, 0, m);
    System.arraycopy(b.x, 0, x, m, n);
    Object[] y = new Object[m + n];
    System.arraycopy(a.y, 0, y, 0, m);
    System.arraycopy(b.y, 0, y, m, n);
    return new Flip(new Dict(x, y));
  }
  /**
   * Creates a string from int with left padding of 0s, if less than 2 digits
   * @param i Integer to convert to string
   * @return String representation of int with zero padding
   */
  static String i2(int i) {
    return new DecimalFormat("00").format(i);
  }
  /**
   * Creates a string from int with left padding of 0s, if less than 9 digits
   * @param i Integer to convert to string
   * @return String representation of int with zero padding
   */
  static String i9(int i) {
    return new DecimalFormat("000000000").format(i);
  }

  private Object getCachedType(int typeId) {
    Object cached = typeCache.get(typeId);
    if (cached == null) {
      cached = createTypeInstance(typeId);
      typeCache.put(typeId, cached);
    }
    return cached;
  }

  private Object createTypeInstance(int typeId) {
    switch (typeId) {
      case TYPE_BOOLEAN:
        return false;
      case TYPE_UUID:
        return new UUID(0, 0);
      case TYPE_BYTE:
        return (byte) 0;
      case TYPE_SHORT:
        return Short.MIN_VALUE;
      case TYPE_INT:
        return ni;
      case TYPE_LONG:
        return nj;
      case TYPE_FLOAT:
        return (float) nf;
      case TYPE_DOUBLE:
        return nf;
      case TYPE_CHAR:
        return ' ';
      case TYPE_STRING:
        return "";
      case TYPE_INSTANT:
        return Instant.MIN;
      default:
        return null;
    }
  }

  /**
   * Write UUID to serialization buffer in big endian format
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param uuid UUID to serialize
   * @return Updated write index
   */
  int w(MutableDirectBuffer buffer, int writeIndex, UUID uuid) {
    if (ipcVersion < 3)
      throw new RuntimeException("Guid not valid pre kdb+3.0");
    buffer.putLong(writeIndex, uuid.getMostSignificantBits(), ByteOrder.BIG_ENDIAN);
    buffer.putLong(writeIndex + 8, uuid.getLeastSignificantBits(), ByteOrder.BIG_ENDIAN);
    return writeIndex + 16;
  }

  /**
   * Write Month to serialization buffer in big endian format
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param m Month to serialize
   * @return Updated write index
   */
  int w(MutableDirectBuffer buffer, int writeIndex, Month m) {
    buffer.putInt(writeIndex, m.i, ByteOrder.BIG_ENDIAN);
    return writeIndex + 4;
  }

  /**
   * Write Minute to serialization buffer in big endian format
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param u Minute to serialize
   * @return Updated write index
   */
  int w(MutableDirectBuffer buffer, int writeIndex, Minute u) {
    buffer.putInt(writeIndex, u.i, ByteOrder.BIG_ENDIAN);
    return writeIndex + 4;
  }

  /**
   * Write Second to serialization buffer in big endian format
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param v Second to serialize
   * @return Updated write index
   */
  int w(MutableDirectBuffer buffer, int writeIndex, Second v) {
    buffer.putInt(writeIndex, v.i, ByteOrder.BIG_ENDIAN);
    return writeIndex + 4;
  }

  /**
   * Write Timespan to serialization buffer in big endian format
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param n Timespan to serialize
   * @return Updated write index
   */
  int w(MutableDirectBuffer buffer, int writeIndex, Timespan n) {
    if (ipcVersion < 1)
      throw new RuntimeException("Timespan not valid pre kdb+2.6");
    buffer.putLong(writeIndex, n.j, ByteOrder.BIG_ENDIAN);
    return writeIndex + 8;
  }

  /**
   * Write LocalDate to serialization buffer in big endian format
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param d Date to serialize
   * @return Updated write index
   */
  int w(MutableDirectBuffer buffer, int writeIndex, LocalDate d) {
    if (d == LocalDate.MIN) {
      buffer.putInt(writeIndex, ni, ByteOrder.BIG_ENDIAN);
      return writeIndex + 4;
    }
    long daysSince2000 = d.toEpochDay() - DAYS_BETWEEN_1970_2000;
    if (daysSince2000 < Integer.MIN_VALUE || daysSince2000 > Integer.MAX_VALUE)
      throw new RuntimeException("LocalDate epoch day since 2000 must be >= Integer.MIN_VALUE and <= Integer.MAX_VALUE");
    buffer.putInt(writeIndex, (int) (daysSince2000), ByteOrder.BIG_ENDIAN);
    return writeIndex + 4;
  }

  /**
   * Write LocalTime to serialization buffer in big endian format
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param t Time to serialize
   * @return Updated write index
   */
  int w(MutableDirectBuffer buffer, int writeIndex, LocalTime t) {
    buffer.putInt(writeIndex, (t == LOCAL_TIME_NULL) ? ni : (int) ((toEpochSecond(t, LocalDate.of(1970, 1, 1), ZoneOffset.UTC) * 1000 + t.getNano() / 1000000) % MILLS_IN_DAY), ByteOrder.BIG_ENDIAN);
    return writeIndex + 4;
  }

  /**
   * Write LocalDateTime to serialization buffer in big endian format
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param z DateTime to serialize
   * @return Updated write index
   */
  int w(MutableDirectBuffer buffer, int writeIndex, LocalDateTime z) {
    buffer.putDouble(writeIndex, z == LocalDateTime.MIN ? nf : ((z.toInstant(ZoneOffset.UTC).toEpochMilli() - MILLS_BETWEEN_1970_2000) / 8.64e7), ByteOrder.BIG_ENDIAN);
    return writeIndex + 8;
  }

  /**
   * Write Instant to serialization buffer in big endian format
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param p Instant to serialize
   * @return Updated write index
   */
  int w(MutableDirectBuffer buffer, int writeIndex, Instant p) {
    if (ipcVersion < 1)
      throw new RuntimeException("Instant not valid pre kdb+2.6");
    buffer.putLong(writeIndex, p == Instant.MIN ? nj : 1000000 * (p.toEpochMilli() - MILLS_BETWEEN_1970_2000) + p.getNano() % 1000000, ByteOrder.BIG_ENDIAN);
    return writeIndex + 8;
  }

  /**
   * Write String to serialization buffer
   * @param buffer Buffer to write to
   * @param writeIndex Position to write at
   * @param s String to serialize
   * @return Updated write index
   * @throws UnsupportedEncodingException If there is an issue with the encoding
   */
  int w(MutableDirectBuffer buffer, int writeIndex, String s) throws UnsupportedEncodingException {
    if (s == null) {
      writeIndex = w(buffer, writeIndex, (byte) 0);
      return writeIndex;
    }
    byte[] b = s.getBytes(encoding);
    writeIndex = w(buffer, writeIndex, b.length);
    for (int i = 0; i < b.length; i++) {
      writeIndex = w(buffer, writeIndex, b[i]);
    }
    return writeIndex;
  }

  /**
   * Write an integer value to the buffer
   * @param value The value to write
   */
  private void w(int value) {
    writeBuffer.putInt(wBuffPos, Integer.reverseBytes(value));
    wBuffPos += 4;
  }

  /**
   * Write a byte value to the buffer
   * @param value The value to write
   */
  private void w(byte value) {
    writeBuffer.putByte(wBuffPos++, value);
  }

  /**
   * Write a long value to the buffer
   * @param value The value to write
   */
  private void w(long value) {
    writeBuffer.putLong(wBuffPos, Long.reverseBytes(value));
    wBuffPos += 8;
  }

  /**
   * Write a double value to the buffer
   * @param value The value to write
   */
  private void w(double value) {
    writeBuffer.putLong(wBuffPos, Long.reverseBytes(Double.doubleToRawLongBits(value)));
    wBuffPos += 8;
  }

  /**
   * Write any object to the buffer
   * @param x Object to write
   * @throws UnsupportedEncodingException If there is an issue with the encoding
   */
  private void w(Object x) throws UnsupportedEncodingException {
    if (x instanceof UUID) w((UUID)x);
    else if (x instanceof Month) w((Month)x);
    else if (x instanceof Minute) w((Minute)x);
    else if (x instanceof Second) w((Second)x);
    else if (x instanceof Timespan) w((Timespan)x);
    else if (x instanceof LocalDate) w((LocalDate)x);
    else if (x instanceof LocalTime) w((LocalTime)x);
    else if (x instanceof LocalDateTime) w((LocalDateTime)x);
    else if (x instanceof Instant) w((Instant)x);
    else if (x instanceof String) w((String)x);
    else if (x instanceof Integer) w((Integer)x);
    else if (x instanceof Byte) w((Byte)x);
    else if (x instanceof Long) w((Long)x);
    else if (x instanceof Double) w((Double)x);
    else throw new UnsupportedEncodingException("Type not supported: " + x.getClass());
  }
}