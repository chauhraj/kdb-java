package com.kx.datatypes;

import java.time.*;

/**
 * Enum representing KDB+ data types and their numeric representations.
 * See data type reference <a href="https://code.kx.com/q/basics/datatypes/">https://code.kx.com/q/basics/datatypes/</a>
 */
public enum DataTypes {
    // Atomic types (negative numbers)
    BOOLEAN(Boolean.class, -1),
    UUID(java.util.UUID.class, -2),
    BYTE(Byte.class, -4),
    SHORT(Short.class, -5),
    INT(Integer.class, -6),
    LONG(Long.class, -7),
    FLOAT(Float.class, -8),
    DOUBLE(Double.class, -9),
    CHAR(Character.class, -10),
    STRING(String.class, -11),
    INSTANT(Instant.class, -12),
    MONTH(Month.class, -13),
    DATE(LocalDate.class, -14),
    DATETIME(LocalDateTime.class, -15),
    TIMESPAN(Timespan.class, -16),
    MINUTE(Minute.class, -17),
    SECOND(Second.class, -18),
    TIME(LocalTime.class, -19),

    // Vector types (positive numbers)
    BOOLEAN_VECTOR(boolean[].class, 1),
    UUID_VECTOR(java.util.UUID[].class, 2),
    BYTE_VECTOR(byte[].class, 4),
    SHORT_VECTOR(short[].class, 5),
    INT_VECTOR(int[].class, 6),
    LONG_VECTOR(long[].class, 7),
    FLOAT_VECTOR(float[].class, 8),
    DOUBLE_VECTOR(double[].class, 9),
    CHAR_VECTOR(char[].class, 10),
    STRING_VECTOR(String[].class, 11),
    INSTANT_VECTOR(Instant[].class, 12),
    MONTH_VECTOR(Month[].class, 13),
    DATE_VECTOR(LocalDate[].class, 14),
    DATETIME_VECTOR(LocalDateTime[].class, 15),
    TIMESPAN_VECTOR(Timespan[].class, 16),
    MINUTE_VECTOR(Minute[].class, 17),
    SECOND_VECTOR(Second[].class, 18),
    TIME_VECTOR(LocalTime[].class, 19),

    // Special types
    FLIP(Flip.class, 98),
    DICT(Dict.class, 99),
    
    // Default type
    OBJECT(Object.class, 0);

    private final Class<?> javaType;
    private final int kdbType;

    /**
     * Constructor for DataTypes enum
     * @param javaType The Java class representing this type
     * @param kdbType The KDB+ numeric type representation
     */
    DataTypes(Class<?> javaType, int kdbType) {
        this.javaType = javaType;
        this.kdbType = kdbType;
    }

    /**
     * Get the KDB+ numeric type representation
     * @return the numeric type used by KDB+
     */
    public int getKdbType() {
        return kdbType;
    }

    /**
     * Get the Java class associated with this type
     * @return the Java class representing this type
     */
    public Class<?> getJavaType() {
        return javaType;
    }

    /**
     * Find the DataType enum value for a given object
     * @param obj The object to find the type for
     * @return The matching DataType enum value
     */
    public static DataTypes fromObject(Object obj) {
        if (obj == null) return OBJECT;
        
        for (DataTypes type : values()) {
            if (type.javaType.isInstance(obj)) {
                return type;
            }
        }
        return OBJECT;
    }

    /**
     * Find the DataType enum value for a given KDB+ type number
     * @param kdbType The KDB+ type number
     * @return The matching DataType enum value
     */
    public static DataTypes fromKdbType(int kdbType) {
        for (DataTypes type : values()) {
            if (type.kdbType == kdbType) {
                return type;
            }
        }
        return OBJECT;
    }
} 