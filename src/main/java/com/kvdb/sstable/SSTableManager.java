package com.kvdb.sstable;

import com.kvdb.core.KeyValue;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * SSTable管理器
 * 负责管理所有SSTable文件的元数据和读取操作
 */
public class SSTableManager {
    private final Path dataDirectory;
    private final List<SSTableMetadata> sstables;
    private final Map<Path, SSTableReader> readerCache;
    private final AtomicLong nextFileId;
    private final ReentrantReadWriteLock lock;
    
    public SSTableManager(Path dataDirectory) throws IOException {
        this.dataDirectory = dataDirectory;
        this.sstables = new CopyOnWriteArrayList<>();
        this.readerCache = new ConcurrentHashMap<>();
        this.nextFileId = new AtomicLong(1);
        this.lock = new ReentrantReadWriteLock();
        
        // 创建数据目录
        Files.createDirectories(dataDirectory);
        
        // 扫描现有的SSTable文件
        scanExistingSSTables();
    }
    
    /**
     * 添加新的SSTable
     */
    public void addSSTable(SSTableMetadata metadata) {
        lock.writeLock().lock();
        try {
            sstables.add(metadata);
            // 按创建时间排序，新的在前面（这样读取时优先读新数据）
            sstables.sort((a, b) -> Long.compare(b.getCreatedTimestamp(), a.getCreatedTimestamp()));
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 移除SSTable
     */
    public boolean removeSSTable(SSTableMetadata metadata) {
        lock.writeLock().lock();
        try {
            boolean removed = sstables.remove(metadata);
            if (removed) {
                // 从缓存中移除对应的reader
                SSTableReader reader = readerCache.remove(metadata.getFilePath());
                if (reader != null) {
                    reader.close();
                }
                
                // 删除物理文件
                try {
                    Files.deleteIfExists(metadata.getFilePath());
                } catch (IOException e) {
                    System.err.println("Failed to delete SSTable file: " + metadata.getFilePath() + ", " + e.getMessage());
                }
            }
            return removed;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 根据key查找值
     */
    public KeyValue get(String key) throws IOException {
        lock.readLock().lock();
        try {
            // 按创建时间倒序遍历SSTable，确保读到最新的数据
            for (SSTableMetadata metadata : sstables) {
                // 首先检查key是否在这个SSTable的范围内
                if (!metadata.mightContainKey(key)) {
                    continue;
                }
                
                // 获取或创建reader
                SSTableReader reader = getOrCreateReader(metadata);
                KeyValue result = reader.get(key);
                
                if (result != null) {
                    return result; // 找到了，返回结果（可能是值或墓碑）
                }
            }
            
            return null; // 所有SSTable中都没找到
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 获取所有SSTable的元数据（只读副本）
     */
    public List<SSTableMetadata> getAllSSTables() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(sstables);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 获取SSTable数量
     */
    public int getSSTableCount() {
        return sstables.size();
    }
    
    /**
     * 生成新的SSTable文件名
     */
    public Path generateSSTablePath() {
        long fileId = nextFileId.getAndIncrement();
        String filename = String.format("sstable_%06d.db", fileId);
        return dataDirectory.resolve(filename);
    }
    
    /**
     * 获取总的文件大小
     */
    public long getTotalFileSize() {
        lock.readLock().lock();
        try {
            return sstables.stream()
                    .mapToLong(SSTableMetadata::getFileSize)
                    .sum();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 获取所有SSTable的条目总数
     */
    public long getTotalEntryCount() {
        lock.readLock().lock();
        try {
            return sstables.stream()
                    .mapToLong(SSTableMetadata::getEntryCount)
                    .sum();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 根据大小对SSTable进行分组（用于Size-Tiered Compaction）
     */
    public Map<Integer, List<SSTableMetadata>> groupSSTablesBySize() {
        lock.readLock().lock();
        try {
            Map<Integer, List<SSTableMetadata>> groups = new HashMap<>();
            
            for (SSTableMetadata metadata : sstables) {
                // 根据文件大小计算层级（简化的分层策略）
                int tier = calculateTier(metadata.getFileSize());
                groups.computeIfAbsent(tier, k -> new ArrayList<>()).add(metadata);
            }
            
            return groups;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 选择需要进行Compaction的SSTable
     */
    public List<SSTableMetadata> selectSSTablesForCompaction(int maxFiles) {
        Map<Integer, List<SSTableMetadata>> groups = groupSSTablesBySize();
        
        // 选择具有最多文件的层级进行Compaction
        int targetTier = -1;
        int maxCount = 0;
        
        for (Map.Entry<Integer, List<SSTableMetadata>> entry : groups.entrySet()) {
            if (entry.getValue().size() > maxCount) {
                maxCount = entry.getValue().size();
                targetTier = entry.getKey();
            }
        }
        
        if (targetTier >= 0 && maxCount >= 2) {
            List<SSTableMetadata> candidates = groups.get(targetTier);
            // 选择最老的文件进行合并
            candidates.sort(Comparator.comparing(SSTableMetadata::getCreatedTimestamp));
            return candidates.subList(0, Math.min(maxFiles, candidates.size()));
        }
        
        return Collections.emptyList();
    }
    
    /**
     * 关闭所有reader
     */
    public void close() {
        lock.writeLock().lock();
        try {
            for (SSTableReader reader : readerCache.values()) {
                reader.close();
            }
            readerCache.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 扫描现有的SSTable文件
     */
    private void scanExistingSSTables() throws IOException {
        if (!Files.exists(dataDirectory)) {
            return;
        }
        
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDirectory, "*.db")) {
            for (Path file : stream) {
                try {
                    SSTableReader reader = new SSTableReader(file);
                    SSTableMetadata metadata = reader.getMetadata();
                    sstables.add(metadata);
                    
                    // 更新文件ID计数器
                    updateNextFileId(file);
                } catch (IOException e) {
                    System.err.println("Failed to load SSTable: " + file + ", " + e.getMessage());
                }
            }
            
            // 按创建时间排序
            sstables.sort((a, b) -> Long.compare(b.getCreatedTimestamp(), a.getCreatedTimestamp()));
        }
    }
    
    /**
     * 更新下一个文件ID
     */
    private void updateNextFileId(Path file) {
        String filename = file.getFileName().toString();
        if (filename.startsWith("sstable_") && filename.endsWith(".db")) {
            try {
                String idStr = filename.substring(8, filename.length() - 3);
                long fileId = Long.parseLong(idStr);
                long current = nextFileId.get();
                if (fileId >= current) {
                    nextFileId.set(fileId + 1);
                }
            } catch (NumberFormatException e) {
                // 忽略无法解析的文件名
            }
        }
    }
    
    /**
     * 获取或创建SSTableReader
     */
    private SSTableReader getOrCreateReader(SSTableMetadata metadata) throws IOException {
        Path filePath = metadata.getFilePath();
        SSTableReader reader = readerCache.get(filePath);
        
        if (reader == null) {
            reader = new SSTableReader(metadata);
            readerCache.put(filePath, reader);
        }
        
        return reader;
    }
    
    /**
     * 根据文件大小计算层级
     */
    private int calculateTier(long fileSize) {
        // 简化的分层策略：每64MB一个层级
        long baseSizeBytes = 64 * 1024 * 1024; // 64MB
        
        if (fileSize <= baseSizeBytes) {
            return 0;
        } else if (fileSize <= baseSizeBytes * 4) {
            return 1;
        } else if (fileSize <= baseSizeBytes * 16) {
            return 2;
        } else {
            return 3;
        }
    }
    
    @Override
    public String toString() {
        return String.format("SSTableManager{dataDir='%s', sstableCount=%d, totalSize=%d bytes}",
                dataDirectory, getSSTableCount(), getTotalFileSize());
    }
}