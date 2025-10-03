package com.kvdb.sstable;

import com.kvdb.core.KeyValue;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * SSTable迭代器
 * 用于顺序遍历SSTable中的键值对
 */
public class SSTableIterator implements Iterator<KeyValue>, AutoCloseable {
    private final DataInputStream dataStream;
    private final String fromKey;
    private final String toKey;
    private KeyValue nextEntry;
    private boolean hasNext;
    private boolean closed;
    
    /**
     * 创建遍历整个SSTable的迭代器
     */
    public SSTableIterator(Path filePath) throws IOException {
        this(filePath, null, null);
    }
    
    /**
     * 创建遍历指定范围的迭代器
     */
    public SSTableIterator(Path filePath, String fromKey, String toKey) throws IOException {
        this.fromKey = fromKey;
        this.toKey = toKey;
        this.closed = false;
        
        BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(filePath));
        this.dataStream = new DataInputStream(bis);
        
        // 如果指定了起始key，需要跳转到合适的位置
        if (fromKey != null) {
            skipToKey(fromKey);
        }
        
        // 读取第一个有效的entry
        advance();
    }
    
    @Override
    public boolean hasNext() {
        return hasNext && !closed;
    }
    
    @Override
    public KeyValue next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        
        KeyValue current = nextEntry;
        advance(); // 准备下一个entry
        return current;
    }
    
    /**
     * 前进到下一个有效的entry
     */
    private void advance() {
        try {
            while (dataStream.available() > 0) {
                KeyValue kv = SSTableSerializer.readKeyValue(dataStream);
                
                // 检查是否在指定范围内
                if (isInRange(kv.getKey())) {
                    nextEntry = kv;
                    hasNext = true;
                    return;
                }
                
                // 如果已经超过了toKey，停止迭代
                if (toKey != null && kv.getKey().compareTo(toKey) > 0) {
                    break;
                }
            }
        } catch (IOException e) {
            // 读取异常或到达文件末尾
        }
        
        // 没有更多有效entry
        nextEntry = null;
        hasNext = false;
    }
    
    /**
     * 检查key是否在指定范围内
     */
    private boolean isInRange(String key) {
        if (fromKey != null && key.compareTo(fromKey) < 0) {
            return false;
        }
        if (toKey != null && key.compareTo(toKey) >= 0) {
            return false;
        }
        return true;
    }
    
    /**
     * 跳转到指定key的位置（简化实现，顺序扫描）
     */
    private void skipToKey(String targetKey) throws IOException {
        while (dataStream.available() > 0) {
            // 先peek一下key，看是否需要跳过
            dataStream.mark(1024); // 标记当前位置
            
            try {
                KeyValue kv = SSTableSerializer.readKeyValue(dataStream);
                if (kv.getKey().compareTo(targetKey) >= 0) {
                    // 找到了合适的位置，重置到这个entry的开始
                    dataStream.reset();
                    break;
                }
            } catch (IOException e) {
                // 如果读取失败，重置位置
                dataStream.reset();
                break;
            }
        }
    }
    
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            dataStream.close();
        }
    }
    
    /**
     * 检查迭代器是否已关闭
     */
    public boolean isClosed() {
        return closed;
    }
}