package com.kvdb.wal;

/**
 * WAL记录类型枚举
 */
public enum WALRecordType {
    PUT((byte) 1),
    DELETE((byte) 2);
    
    private final byte value;
    
    WALRecordType(byte value) {
        this.value = value;
    }
    
    public byte getValue() {
        return value;
    }
    
    public static WALRecordType fromByte(byte value) {
        for (WALRecordType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown WAL record type: " + value);
    }
}