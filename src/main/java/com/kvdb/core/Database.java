package com.kvdb.core;

import com.kvdb.AppendOnlyKVDB.DatabaseStats;
import com.kvdb.compaction.CompactionService;
import com.kvdb.memtable.MemTable;
import com.kvdb.memtable.MemTableSnapshot;
import com.kvdb.sstable.SSTableManager;
import com.kvdb.sstable.SSTableMetadata;
import com.kvdb.sstable.SSTableWriter;
import com.kvdb.wal.WALRecord;
import com.kvdb.wal.WriteAheadLog;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 数据库核心实现类
 * 整合MemTable、SSTable、WAL等组件
 */
public class Database {
    private final Path dataDirectory;
    private final DatabaseConfig config;
    
    // 核心组件
    private volatile MemTable activeMemTable;
    private volatile MemTable immutableMemTable;
    private final SSTableManager sstableManager;
    private final WriteAheadLog wal;
    private final CompactionService compactionService;
    
    // 并发控制
    private final ReentrantReadWriteLock memTableLock;
    private final ExecutorService flushExecutor;
    
    // 统计信息
    private final AtomicLong totalReads;
    private final AtomicLong totalWrites;
    
    // 状态
    private volatile boolean closed;
    
    public Database(Path dataDirectory, DatabaseConfig config) throws IOException {
        this.dataDirectory = dataDirectory;
        this.config = config;
        this.memTableLock = new ReentrantReadWriteLock();
        this.flushExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "MemTable-Flush-Thread");
            t.setDaemon(true);
            return t;
        });
        this.totalReads = new AtomicLong(0);
        this.totalWrites = new AtomicLong(0);
        this.closed = false;
        
        // 初始化组件
        this.sstableManager = new SSTableManager(dataDirectory);
        
        // 初始化WAL
        Path walPath = dataDirectory.resolve("database.wal");
        this.wal = config.isEnableWAL() ? 
            new WriteAheadLog(walPath, config.getWalSyncInterval()) : null;
        
        // 初始化MemTable
        this.activeMemTable = new MemTable();
        this.immutableMemTable = null;
        
        // 初始化Compaction服务
        this.compactionService = new CompactionService(sstableManager, config);
        
        // 恢复数据（如果存在WAL）
        if (wal != null) {
            recoverFromWAL();
        }
        
        // 启动后台Compaction服务
        compactionService.start();
    }
    
    /**
     * 设置键值对
     */
    public void set(String key, String value) throws IOException {
        if (closed) {
            throw new IllegalStateException("Database is closed");
        }
        
        totalWrites.incrementAndGet();
        
        memTableLock.writeLock().lock();
        try {
            // 写WAL（如果启用）
            if (wal != null) {
                wal.logPut(key, value);
            }
            
            // 写入活跃MemTable
            activeMemTable.put(key, value);
            
            // 检查是否需要刷盘
            if (shouldFlushMemTable()) {
                triggerMemTableFlush();
            }
        } finally {
            memTableLock.writeLock().unlock();
        }
    }
    
    /**
     * 获取值
     */
    public String get(String key) throws IOException {
        if (closed) {
            throw new IllegalStateException("Database is closed");
        }
        
        totalReads.incrementAndGet();
        
        memTableLock.readLock().lock();
        try {
            // 1. 查找活跃MemTable
            KeyValue kv = activeMemTable.get(key);
            if (kv != null) {
                return kv.isTombstone() ? null : kv.getValue();
            }
            
            // 2. 查找不可变MemTable（如果存在）
            if (immutableMemTable != null) {
                kv = immutableMemTable.get(key);
                if (kv != null) {
                    return kv.isTombstone() ? null : kv.getValue();
                }
            }
        } finally {
            memTableLock.readLock().unlock();
        }
        
        // 3. 查找SSTable
        KeyValue kv = sstableManager.get(key);
        if (kv != null) {
            return kv.isTombstone() ? null : kv.getValue();
        }
        
        return null; // 未找到
    }
    
    /**
     * 删除键（逻辑删除）
     */
    public void delete(String key) throws IOException {
        if (closed) {
            throw new IllegalStateException("Database is closed");
        }
        
        totalWrites.incrementAndGet();
        
        memTableLock.writeLock().lock();
        try {
            // 写WAL（如果启用）
            if (wal != null) {
                wal.logDelete(key);
            }
            
            // 写入墓碑标记
            activeMemTable.delete(key);
            
            // 检查是否需要刷盘
            if (shouldFlushMemTable()) {
                triggerMemTableFlush();
            }
        } finally {
            memTableLock.writeLock().unlock();
        }
    }
    
    /**
     * 手动触发Compaction
     */
    public void compact() throws IOException {
        compactionService.triggerCompaction();
    }
    
    /**
     * 获取数据库统计信息
     */
    public DatabaseStats getStats() {
        memTableLock.readLock().lock();
        try {
            int memTableSize = activeMemTable.size();
            if (immutableMemTable != null) {
                memTableSize += immutableMemTable.size();
            }
            
            return new DatabaseStats(
                memTableSize,
                sstableManager.getSSTableCount(),
                totalWrites.get(),
                totalReads.get()
            );
        } finally {
            memTableLock.readLock().unlock();
        }
    }
    
    /**
     * 关闭数据库
     */
    public void close() throws IOException {
        if (closed) {
            return;
        }
        
        closed = true;
        
        // 刷盘当前的MemTable
        flushActiveMemTable();
        
        // 关闭WAL
        if (wal != null) {
            wal.close();
        }
        
        // 关闭SSTable管理器
        sstableManager.close();
        
        // 关闭Compaction服务
        compactionService.shutdown();
        
        // 关闭线程池
        flushExecutor.shutdown();
    }
    
    /**
     * 检查是否需要刷盘MemTable
     */
    private boolean shouldFlushMemTable() {
        return activeMemTable.size() >= config.getMemTableFlushThreshold();
    }
    
    /**
     * 触发MemTable刷盘
     */
    private void triggerMemTableFlush() {
        // 冻结当前MemTable
        immutableMemTable = activeMemTable;
        activeMemTable = new MemTable();
        
        // 异步刷盘
        flushExecutor.submit(() -> {
            try {
                flushImmutableMemTable();
            } catch (IOException e) {
                System.err.println("Failed to flush MemTable: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }
    
    /**
     * 刷盘活跃MemTable
     */
    private void flushActiveMemTable() throws IOException {
        memTableLock.writeLock().lock();
        try {
            if (!activeMemTable.isEmpty()) {
                immutableMemTable = activeMemTable;
                activeMemTable = new MemTable();
                flushImmutableMemTable();
            }
        } finally {
            memTableLock.writeLock().unlock();
        }
    }
    
    /**
     * 刷盘不可变MemTable
     */
    private void flushImmutableMemTable() throws IOException {
        MemTable toFlush;
        
        memTableLock.readLock().lock();
        try {
            toFlush = immutableMemTable;
            if (toFlush == null || toFlush.isEmpty()) {
                return;
            }
        } finally {
            memTableLock.readLock().unlock();
        }
        
        // 创建MemTable快照
        MemTableSnapshot snapshot = toFlush.createSnapshot();
        
        // 生成SSTable文件路径
        Path sstablePath = sstableManager.generateSSTablePath();
        
        // 写入SSTable
        SSTableWriter writer = new SSTableWriter(sstablePath);
        try {
            writer.writeFromMemTable(snapshot);
            SSTableMetadata metadata = writer.finish();
            
            // 添加到SSTable管理器
            sstableManager.addSSTable(metadata);
            
            // 清空不可变MemTable
            memTableLock.writeLock().lock();
            try {
                if (immutableMemTable == toFlush) {
                    immutableMemTable = null;
                }
            } finally {
                memTableLock.writeLock().unlock();
            }
            
            // 清理WAL（如果启用）
            if (wal != null) {
                wal.clear();
            }
            
            System.out.println("Flushed MemTable to SSTable: " + sstablePath.getFileName());
            
        } catch (IOException e) {
            writer.cancel();
            throw e;
        }
    }
    
    /**
     * 从WAL恢复数据
     */
    private void recoverFromWAL() throws IOException {
        if (wal == null) {
            return;
        }
        
        List<WALRecord> records = wal.recover();
        if (records.isEmpty()) {
            return;
        }
        
        System.out.println("Recovering " + records.size() + " records from WAL");
        
        for (WALRecord record : records) {
            KeyValue kv = record.toKeyValue();
            activeMemTable.put(kv);
        }
        
        System.out.println("WAL recovery completed");
    }
}