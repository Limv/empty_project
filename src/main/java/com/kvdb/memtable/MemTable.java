package com.kvdb.memtable;

import com.kvdb.core.KeyValue;

import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MemTable - 内存表实现
 * 使用ConcurrentSkipListMap保证线程安全和有序性
 */
public class MemTable {
    private final ConcurrentSkipListMap<String, KeyValue> data;
    private final AtomicInteger size;
    private final AtomicLong memoryUsage;
    private final long createdTimestamp;
    
    public MemTable() {
        this.data = new ConcurrentSkipListMap<>();
        this.size = new AtomicInteger(0);
        this.memoryUsage = new AtomicLong(0);
        this.createdTimestamp = System.currentTimeMillis();
    }
    
    /**
     * 插入键值对
     */
    public void put(String key, String value) {
        KeyValue kv = new KeyValue(key, value);
        put(kv);
    }
    
    /**
     * 插入KeyValue对象
     */
    public void put(KeyValue kv) {
        KeyValue oldValue = data.put(kv.getKey(), kv);
        
        if (oldValue == null) {
            size.incrementAndGet();
        }
        
        // 估算内存使用量（简化计算）
        long keySize = kv.getKey().length() * 2; // UTF-16编码，每字符2字节
        long valueSize = kv.getValue() != null ? kv.getValue().length() * 2 : 0;
        long kvSize = keySize + valueSize + 64; // 额外的对象开销
        
        if (oldValue != null) {
            long oldKeySize = oldValue.getKey().length() * 2;
            long oldValueSize = oldValue.getValue() != null ? oldValue.getValue().length() * 2 : 0;
            long oldKvSize = oldKeySize + oldValueSize + 64;
            memoryUsage.addAndGet(kvSize - oldKvSize);
        } else {
            memoryUsage.addAndGet(kvSize);
        }
    }
    
    /**
     * 插入删除标记（墓碑）
     */
    public void delete(String key) {
        KeyValue tombstone = KeyValue.createTombstone(key);
        put(tombstone);
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
     * 获取当前大小（条目数）
     */
    public int size() {
        return size.get();
    }
    
    /**
     * 检查是否为空
     */
    public boolean isEmpty() {
        return data.isEmpty();
    }
    
    /**
     * 获取估算的内存使用量（字节）
     */
    public long getMemoryUsage() {
        return memoryUsage.get();
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
    
    /**
     * 清空MemTable
     */
    public void clear() {
        data.clear();
        size.set(0);
        memoryUsage.set(0);
    }
    
    /**
     * 创建MemTable的只读快照
     */
    public MemTableSnapshot createSnapshot() {
        return new MemTableSnapshot(data, size.get(), memoryUsage.get(), createdTimestamp);
    }
    
    @Override
    public String toString() {
        return String.format("MemTable{size=%d, memoryUsage=%d bytes, created=%d}", 
                size.get(), memoryUsage.get(), createdTimestamp);
    }
}