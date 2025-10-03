package com.kvdb.core;

/**
 * 键值对数据结构
 */
public class KeyValue {
    private final String key;
    private final String value;
    private final boolean tombstone;  // 墓碑标记，用于逻辑删除
    private final long timestamp;     // 时间戳，用于确定数据新旧
    
    public KeyValue(String key, String value) {
        this(key, value, false, System.currentTimeMillis());
    }
    
    public KeyValue(String key, String value, boolean tombstone) {
        this(key, value, tombstone, System.currentTimeMillis());
    }
    
    public KeyValue(String key, String value, boolean tombstone, long timestamp) {
        this.key = key;
        this.value = value;
        this.tombstone = tombstone;
        this.timestamp = timestamp;
    }
    
    /**
     * 创建删除标记（墓碑）
     */
    public static KeyValue createTombstone(String key) {
        return new KeyValue(key, null, true);
    }
    
    public String getKey() {
        return key;
    }
    
    public String getValue() {
        return value;
    }
    
    public boolean isTombstone() {
        return tombstone;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    /**
     * 检查这个KeyValue是否比另一个更新
     */
    public boolean isNewerThan(KeyValue other) {
        return this.timestamp > other.timestamp;
    }
    
    @Override
    public String toString() {
        if (tombstone) {
            return String.format("KeyValue{key='%s', TOMBSTONE, timestamp=%d}", key, timestamp);
        } else {
            return String.format("KeyValue{key='%s', value='%s', timestamp=%d}", key, value, timestamp);
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        KeyValue keyValue = (KeyValue) obj;
        return tombstone == keyValue.tombstone &&
                timestamp == keyValue.timestamp &&
                key.equals(keyValue.key) &&
                (value != null ? value.equals(keyValue.value) : keyValue.value == null);
    }
    
    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (tombstone ? 1 : 0);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }
}