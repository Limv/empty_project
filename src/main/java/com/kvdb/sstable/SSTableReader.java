package com.kvdb.sstable;

import com.kvdb.core.KeyValue;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * SSTable读取器
 * 负责从SSTable文件中读取数据
 */
public class SSTableReader {
    private final Path filePath;
    private final SSTableMetadata metadata;
    private final List<SSTableSerializer.IndexEntry> indexEntries;
    private final long indexOffset;
    
    public SSTableReader(Path filePath) throws IOException {
        this.filePath = filePath;
        this.metadata = loadMetadata();
        this.indexOffset = loadIndexOffset();
        this.indexEntries = loadIndexEntries();
    }
    
    public SSTableReader(SSTableMetadata metadata) throws IOException {
        this.filePath = metadata.getFilePath();
        this.metadata = metadata;
        this.indexOffset = loadIndexOffset();
        this.indexEntries = loadIndexEntries();
    }
    
    /**
     * 根据key查找值
     */
    public KeyValue get(String key) throws IOException {
        // 首先检查key是否在范围内
        if (!metadata.mightContainKey(key)) {
            return null;
        }
        
        // 在索引中查找key的位置
        int index = findIndexPosition(key);
        if (index < 0) {
            return null; // key不存在
        }
        
        // 从找到的位置开始顺序扫描
        return scanFromPosition(key, indexEntries.get(index).getOffset());
    }
    
    /**
     * 获取所有键值对的迭代器
     */
    public SSTableIterator iterator() throws IOException {
        return new SSTableIterator(filePath);
    }
    
    /**
     * 获取指定范围的迭代器
     */
    public SSTableIterator rangeIterator(String fromKey, String toKey) throws IOException {
        return new SSTableIterator(filePath, fromKey, toKey);
    }
    
    /**
     * 获取SSTable元数据
     */
    public SSTableMetadata getMetadata() {
        return metadata;
    }
    
    /**
     * 在索引中查找key的位置
     * 返回小于等于key的最大索引位置
     */
    private int findIndexPosition(String key) {
        int left = 0;
        int right = indexEntries.size() - 1;
        int result = -1;
        
        while (left <= right) {
            int mid = left + (right - left) / 2;
            String midKey = indexEntries.get(mid).getKey();
            
            int cmp = midKey.compareTo(key);
            if (cmp == 0) {
                return mid; // 找到精确匹配
            } else if (cmp < 0) {
                result = mid; // 记录小于等于key的位置
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        
        return result;
    }
    
    /**
     * 从指定位置开始扫描，查找特定的key
     */
    private KeyValue scanFromPosition(String targetKey, long startOffset) throws IOException {
        try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(filePath));
             DataInputStream dis = new DataInputStream(bis)) {
            
            // 跳转到指定位置
            dis.skip(startOffset);
            
            // 顺序扫描直到找到key或超出范围
            while (dis.available() > 0) {
                long currentPos = startOffset + (Files.size(filePath) - dis.available());
                
                // 如果已经到达索引区，停止扫描
                if (currentPos >= indexOffset) {
                    break;
                }
                
                KeyValue kv = SSTableSerializer.readKeyValue(dis);
                
                int cmp = kv.getKey().compareTo(targetKey);
                if (cmp == 0) {
                    return kv; // 找到目标key
                } else if (cmp > 0) {
                    break; // 已经超过目标key，不存在
                }
                // cmp < 0, 继续扫描
            }
        }
        
        return null; // 没找到
    }
    
    /**
     * 加载索引偏移量
     */
    private long loadIndexOffset() throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
            // 索引偏移量存储在倒数第二个8字节（最后8字节是元数据的开始位置）
            long fileSize = raf.length();
            raf.seek(fileSize - 8 - getMetadataSize());
            return raf.readLong();
        }
    }
    
    /**
     * 加载索引条目
     */
    private List<SSTableSerializer.IndexEntry> loadIndexEntries() throws IOException {
        List<SSTableSerializer.IndexEntry> entries = new ArrayList<>();
        
        try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(filePath));
             DataInputStream dis = new DataInputStream(bis)) {
            
            // 跳转到索引区开始位置
            dis.skip(indexOffset);
            
            // 读取直到到达索引偏移量存储位置
            long metadataStart = Files.size(filePath) - 8 - getMetadataSize();
            long currentPos = indexOffset;
            
            while (currentPos < metadataStart) {
                // 读取key长度
                int keyLen = dis.readInt();
                currentPos += 4;
                
                // 读取key
                byte[] keyBytes = new byte[keyLen];
                dis.readFully(keyBytes);
                String key = new String(keyBytes, "UTF-8");
                currentPos += keyLen;
                
                // 读取offset
                long offset = dis.readLong();
                currentPos += 8;
                
                entries.add(new SSTableSerializer.IndexEntry(key, offset));
            }
        }
        
        return entries;
    }
    
    /**
     * 加载元数据
     */
    private SSTableMetadata loadMetadata() throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
            long fileSize = raf.length();
            
            // 定位到元数据区
            raf.seek(fileSize - getMetadataSize());
            
            // 读取条目数量
            int entryCount = raf.readInt();
            
            // 读取最小key
            int minKeyLen = raf.readInt();
            String minKey = null;
            if (minKeyLen > 0) {
                byte[] minKeyBytes = new byte[minKeyLen];
                raf.readFully(minKeyBytes);
                minKey = new String(minKeyBytes, "UTF-8");
            }
            
            // 读取最大key
            int maxKeyLen = raf.readInt();
            String maxKey = null;
            if (maxKeyLen > 0) {
                byte[] maxKeyBytes = new byte[maxKeyLen];
                raf.readFully(maxKeyBytes);
                maxKey = new String(maxKeyBytes, "UTF-8");
            }
            
            // 读取创建时间戳
            long createdTimestamp = raf.readLong();
            
            return new SSTableMetadata(filePath, minKey, maxKey, entryCount, fileSize, createdTimestamp);
        }
    }
    
    /**
     * 获取元数据区的固定大小（这是一个简化的实现）
     * 实际实现中应该更精确地计算
     */
    private int getMetadataSize() {
        // 这里简化处理，假设元数据区最大1KB
        // 实际实现中应该在文件末尾记录元数据大小
        return 1024;
    }
    
    /**
     * 关闭reader（当前实现中不需要显式关闭）
     */
    public void close() {
        // 当前实现不需要显式关闭资源
    }
}