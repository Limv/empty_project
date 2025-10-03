package com.kvdb.compaction;

import com.kvdb.core.KeyValue;
import com.kvdb.sstable.SSTableIterator;
import com.kvdb.sstable.SSTableMetadata;

import java.io.IOException;
import java.util.*;

/**
 * 多路归并迭代器
 * 用于Compaction过程中合并多个SSTable的数据
 */
public class MergeIterator implements Iterator<KeyValue>, AutoCloseable {
    private final PriorityQueue<IteratorWrapper> heap;
    private final Set<SSTableIterator> iterators;
    private KeyValue nextKeyValue;
    private boolean hasNext;
    private boolean closed;
    
    public MergeIterator(List<SSTableMetadata> sstables) throws IOException {
        this.heap = new PriorityQueue<>(sstables.size(), 
            Comparator.comparing(wrapper -> wrapper.current.getKey()));
        this.iterators = new HashSet<>();
        this.closed = false;
        
        // 为每个SSTable创建迭代器
        for (SSTableMetadata metadata : sstables) {
            SSTableIterator iterator = new SSTableIterator(metadata.getFilePath());
            iterators.add(iterator);
            
            if (iterator.hasNext()) {
                KeyValue kv = iterator.next();
                heap.offer(new IteratorWrapper(iterator, kv));
            } else {
                iterator.close();
            }
        }
        
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
        
        KeyValue current = nextKeyValue;
        advance();
        return current;
    }
    
    /**
     * 前进到下一个键值对
     * 处理相同key的合并逻辑：保留最新的值，应用删除标记
     */
    private void advance() {
        if (heap.isEmpty()) {
            hasNext = false;
            nextKeyValue = null;
            return;
        }
        
        // 取出堆顶元素
        IteratorWrapper wrapper = heap.poll();
        KeyValue current = wrapper.current;
        String currentKey = current.getKey();
        
        // 处理相同key的多个版本
        List<KeyValue> sameKeyValues = new ArrayList<>();
        sameKeyValues.add(current);
        
        // 继续取出相同key的所有值
        while (!heap.isEmpty() && heap.peek().current.getKey().equals(currentKey)) {
            IteratorWrapper nextWrapper = heap.poll();
            sameKeyValues.add(nextWrapper.current);
            
            // 将迭代器重新放入堆中（如果还有数据）
            if (nextWrapper.iterator.hasNext()) {
                try {
                    KeyValue nextKv = nextWrapper.iterator.next();
                    heap.offer(new IteratorWrapper(nextWrapper.iterator, nextKv));
                } catch (Exception e) {
                    // 迭代器出错，关闭它
                    try {
                        nextWrapper.iterator.close();
                    } catch (IOException ignored) {}
                }
            } else {
                // 迭代器已结束，关闭它
                try {
                    nextWrapper.iterator.close();
                } catch (IOException ignored) {}
            }
        }
        
        // 将原迭代器重新放入堆中（如果还有数据）
        if (wrapper.iterator.hasNext()) {
            try {
                KeyValue nextKv = wrapper.iterator.next();
                heap.offer(new IteratorWrapper(wrapper.iterator, nextKv));
            } catch (Exception e) {
                // 迭代器出错，关闭它
                try {
                    wrapper.iterator.close();
                } catch (IOException ignored) {}
            }
        } else {
            // 迭代器已结束，关闭它
            try {
                wrapper.iterator.close();
            } catch (IOException ignored) {}
        }
        
        // 解决相同key的冲突：选择最新的值
        KeyValue resolved = resolveSameKeyValues(sameKeyValues);
        
        // 如果解析结果是墓碑且没有实际值，跳过这个key
        if (resolved.isTombstone()) {
            advance(); // 递归调用，跳过墓碑
        } else {
            nextKeyValue = resolved;
            hasNext = true;
        }
    }
    
    /**
     * 解决相同key的多个值的冲突
     * 规则：保留时间戳最新的值，如果最新的是墓碑则整个key被删除
     */
    private KeyValue resolveSameKeyValues(List<KeyValue> values) {
        if (values.size() == 1) {
            return values.get(0);
        }
        
        // 按时间戳排序，最新的在前
        values.sort((a, b) -> Long.compare(b.getTimestamp(), a.getTimestamp()));
        
        // 返回最新的值
        return values.get(0);
    }
    
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        
        closed = true;
        
        // 关闭所有迭代器
        for (SSTableIterator iterator : iterators) {
            try {
                iterator.close();
            } catch (IOException e) {
                // 记录但不抛出异常
                System.err.println("Error closing iterator: " + e.getMessage());
            }
        }
        
        // 清空堆中的迭代器
        while (!heap.isEmpty()) {
            IteratorWrapper wrapper = heap.poll();
            try {
                wrapper.iterator.close();
            } catch (IOException e) {
                System.err.println("Error closing iterator: " + e.getMessage());
            }
        }
    }
    
    /**
     * 迭代器包装器，用于在优先队列中排序
     */
    private static class IteratorWrapper {
        final SSTableIterator iterator;
        final KeyValue current;
        
        IteratorWrapper(SSTableIterator iterator, KeyValue current) {
            this.iterator = iterator;
            this.current = current;
        }
    }
}