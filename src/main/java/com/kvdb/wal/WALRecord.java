package com.kvdb.wal;

import com.kvdb.core.KeyValue;

/**
 * WAL记录
 * 表示一个写操作的日志记录
 */
public class WALRecord {
    private final WALRecordType type;
    private final String key;
    private final String value;
    private final long timestamp;
    private final long sequenceNumber;
    
    public WALRecord(WALRecordType type, String key, String value, long timestamp, long sequenceNumber) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.sequenceNumber = sequenceNumber;
    }
    
    /**
     * 从PUT操作创建WAL记录
     */
    public static WALRecord createPut(String key, String value, long sequenceNumber) {
        return new WALRecord(WALRecordType.PUT, key, value, System.currentTimeMillis(), sequenceNumber);
    }
    
    /**
     * 从DELETE操作创建WAL记录
     */
    public static WALRecord createDelete(String key, long sequenceNumber) {
        return new WALRecord(WALRecordType.DELETE, key, null, System.currentTimeMillis(), sequenceNumber);
    }
    
    /**
     * 转换为KeyValue对象
     */
    public KeyValue toKeyValue() {
        switch (type) {
            case PUT:
                return new KeyValue(key, value, false, timestamp);
            case DELETE:
                return KeyValue.createTombstone(key);
            default:
                throw new IllegalStateException("Unknown record type: " + type);
        }
    }
    
    public WALRecordType getType() {
        return type;
    }
    
    public String getKey() {
        return key;
    }
    
    public String getValue() {
        return value;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public long getSequenceNumber() {
        return sequenceNumber;
    }
    
    @Override
    public String toString() {
        return String.format("WALRecord{type=%s, key='%s', value='%s', timestamp=%d, seq=%d}",
                type, key, value, timestamp, sequenceNumber);
    }
}