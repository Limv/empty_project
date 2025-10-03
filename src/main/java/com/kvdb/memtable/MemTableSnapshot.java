package com.kvdb.memtable;

import com.kvdb.core.KeyValue;

import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * MemTable的只读快照
 * 用于在刷盘过程中保持数据的一致性
 */
public class MemTableSnapshot {
    private final ConcurrentSkipListMap<String, KeyValue> data;
    private final int size;
    private final long memoryUsage;
    private final long createdTimestamp;
    
    public MemTableSnapshot(ConcurrentSkipListMap<String, KeyValue> data, int size, 
                           long memoryUsage, long createdTimestamp) {
        // 创建数据的副本，确保快照的不可变性
        this.data = new ConcurrentSkipListMap<>(data);
        this.size = size;
        this.memoryUsage = memoryUsage;
        this.createdTimestamp = createdTimestamp;
    }
    
    /**
     * 获取值
     */
    public KeyValue get(String key) {
        return data.get(key);
    }
    
    /**
     * 检查是否包含指定键
     */
    public boolean containsKey(String key) {
        return data.containsKey(key);
    }
    
    /**
     * 获取大小
     */
    public int size() {
        return size;
    }
    
    /**
     * 检查是否为空
     */
    public boolean isEmpty() {
        return data.isEmpty();
    }
    
    /**
     * 获取内存使用量
     */
    public long getMemoryUsage() {
        return memoryUsage;
    }
    
    /**
     * 获取创建时间戳
     */
    public long getCreatedTimestamp() {
        return createdTimestamp;
    }
    
    /**
     * 获取所有键值对的迭代器（按键排序）
     */
    public Iterator<KeyValue> iterator() {
        return data.values().iterator();
    }
    
    /**
     * 获取指定范围内的键值对迭代器
     */
    public Iterator<KeyValue> rangeIterator(String fromKey, String toKey) {
        if (fromKey == null && toKey == null) {
            return iterator();
        } else if (fromKey == null) {
            return data.headMap(toKey, false).values().iterator();
        } else if (toKey == null) {
            return data.tailMap(fromKey, true).values().iterator();
        } else {
            return data.subMap(fromKey, true, toKey, false).values().iterator();
        }
    }
    
    @Override
    public String toString() {
        return String.format("MemTableSnapshot{size=%d, memoryUsage=%d bytes, created=%d}", 
                size, memoryUsage, createdTimestamp);
    }
}