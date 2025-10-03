package com.kvdb.sstable;

import com.kvdb.core.KeyValue;
import com.kvdb.memtable.MemTableSnapshot;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * SSTable写入器
 * 负责将MemTable数据刷盘为SSTable文件
 */
public class SSTableWriter {
    private final Path filePath;
    private final BufferedOutputStream dataOutputStream;
    private final DataOutputStream dataStream;
    private final List<SSTableSerializer.IndexEntry> indexEntries;
    private long currentOffset;
    private String minKey;
    private String maxKey;
    private int entryCount;
    
    public SSTableWriter(Path filePath) throws IOException {
        this.filePath = filePath;
        this.dataOutputStream = new BufferedOutputStream(Files.newOutputStream(filePath));
        this.dataStream = new DataOutputStream(dataOutputStream);
        this.indexEntries = new ArrayList<>();
        this.currentOffset = 0;
        this.entryCount = 0;
    }
    
    /**
     * 写入一个键值对
     */
    public void write(KeyValue kv) throws IOException {
        String key = kv.getKey();
        
        // 更新min/max key
        if (minKey == null || key.compareTo(minKey) < 0) {
            minKey = key;
        }
        if (maxKey == null || key.compareTo(maxKey) > 0) {
            maxKey = key;
        }
        
        // 记录当前偏移量用于索引
        long entryOffset = currentOffset;
        
        // 序列化并写入数据
        SSTableSerializer.writeKeyValue(dataStream, kv);
        
        // 更新当前偏移量
        long entrySize = calculateEntrySize(kv);
        currentOffset += entrySize;
        
        // 添加到索引（这里简化为每个key都建索引，实际实现可以间隔建索引）
        indexEntries.add(new SSTableSerializer.IndexEntry(key, entryOffset));
        
        entryCount++;
    }
    
    /**
     * 从MemTableSnapshot写入所有数据
     */
    public void writeFromMemTable(MemTableSnapshot snapshot) throws IOException {
        Iterator<KeyValue> iterator = snapshot.iterator();
        while (iterator.hasNext()) {
            write(iterator.next());
        }
    }
    
    /**
     * 完成写入并关闭文件
     * 写入格式: [数据区][索引偏移量:8][索引区][元数据]
     */
    public SSTableMetadata finish() throws IOException {
        // 记录索引开始的偏移量
        long indexOffset = currentOffset;
        
        // 写入索引区
        for (SSTableSerializer.IndexEntry entry : indexEntries) {
            byte[] indexData = SSTableSerializer.serializeIndexEntry(entry.getKey(), entry.getOffset());
            dataStream.write(indexData);
            currentOffset += indexData.length;
        }
        
        // 写入索引偏移量（用于快速定位索引区）
        dataStream.writeLong(indexOffset);
        currentOffset += 8;
        
        // 写入元数据
        writeMetadata();
        
        // 关闭流
        dataStream.close();
        dataOutputStream.close();
        
        // 返回SSTable元数据
        return new SSTableMetadata(
            filePath,
            minKey,
            maxKey,
            entryCount,
            Files.size(filePath),
            System.currentTimeMillis()
        );
    }
    
    /**
     * 写入元数据区
     * 格式: [entryCount:4][minKeyLen:4][minKey][maxKeyLen:4][maxKey][timestamp:8]
     */
    private void writeMetadata() throws IOException {
        // 写入条目数量
        dataStream.writeInt(entryCount);
        
        // 写入最小key
        if (minKey != null) {
            byte[] minKeyBytes = minKey.getBytes("UTF-8");
            dataStream.writeInt(minKeyBytes.length);
            dataStream.write(minKeyBytes);
        } else {
            dataStream.writeInt(0);
        }
        
        // 写入最大key
        if (maxKey != null) {
            byte[] maxKeyBytes = maxKey.getBytes("UTF-8");
            dataStream.writeInt(maxKeyBytes.length);
            dataStream.write(maxKeyBytes);
        } else {
            dataStream.writeInt(0);
        }
        
        // 写入创建时间戳
        dataStream.writeLong(System.currentTimeMillis());
    }
    
    /**
     * 计算单个条目的大小（估算）
     */
    private long calculateEntrySize(KeyValue kv) {
        int keySize = kv.getKey().getBytes().length;
        int valueSize = kv.getValue() != null ? kv.getValue().getBytes().length : 0;
        // 4(keyLen) + keySize + 4(valueLen) + valueSize + 1(tombstone) + 8(timestamp)
        return 4 + keySize + 4 + valueSize + 1 + 8;
    }
    
    /**
     * 取消写入，删除部分写入的文件
     */
    public void cancel() throws IOException {
        try {
            dataStream.close();
            dataOutputStream.close();
        } catch (IOException e) {
            // 忽略关闭时的异常
        }
        
        // 删除部分写入的文件
        Files.deleteIfExists(filePath);
    }
}