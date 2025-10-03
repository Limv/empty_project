package com.kvdb.sstable;

import java.nio.file.Path;

/**
 * SSTable元数据
 * 包含文件路径、键范围、大小等信息
 */
public class SSTableMetadata {
    private final Path filePath;
    private final String minKey;
    private final String maxKey;
    private final int entryCount;
    private final long fileSize;
    private final long createdTimestamp;
    
    public SSTableMetadata(Path filePath, String minKey, String maxKey, 
                          int entryCount, long fileSize, long createdTimestamp) {
        this.filePath = filePath;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.entryCount = entryCount;
        this.fileSize = fileSize;
        this.createdTimestamp = createdTimestamp;
    }
    
    public Path getFilePath() {
        return filePath;
    }
    
    public String getMinKey() {
        return minKey;
    }
    
    public String getMaxKey() {
        return maxKey;
    }
    
    public int getEntryCount() {
        return entryCount;
    }
    
    public long getFileSize() {
        return fileSize;
    }
    
    public long getCreatedTimestamp() {
        return createdTimestamp;
    }
    
    /**
     * 检查给定的key是否可能在这个SSTable的范围内
     */
    public boolean mightContainKey(String key) {
        if (minKey == null || maxKey == null) {
            return true; // 如果没有范围信息，假设可能包含
        }
        return key.compareTo(minKey) >= 0 && key.compareTo(maxKey) <= 0;
    }
    
    /**
     * 检查两个SSTable的key范围是否有重叠
     */
    public boolean hasKeyOverlapWith(SSTableMetadata other) {
        if (this.minKey == null || this.maxKey == null || 
            other.minKey == null || other.maxKey == null) {
            return true; // 如果没有范围信息，假设有重叠
        }
        
        // 检查是否有重叠：A的最大值 >= B的最小值 && A的最小值 <= B的最大值
        return this.maxKey.compareTo(other.minKey) >= 0 && 
               this.minKey.compareTo(other.maxKey) <= 0;
    }
    
    /**
     * 获取文件名（不含路径）
     */
    public String getFileName() {
        return filePath.getFileName().toString();
    }
    
    @Override
    public String toString() {
        return String.format(
            "SSTableMetadata{file='%s', keyRange=[%s, %s], entries=%d, size=%d bytes, created=%d}",
            getFileName(), minKey, maxKey, entryCount, fileSize, createdTimestamp
        );
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        SSTableMetadata that = (SSTableMetadata) obj;
        return filePath.equals(that.filePath);
    }
    
    @Override
    public int hashCode() {
        return filePath.hashCode();
    }
}