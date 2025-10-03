package com.kvdb;

import com.kvdb.core.Database;
import com.kvdb.core.DatabaseConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Append-Only Key-Value Database - 主要API入口
 * 基于LSM-Tree思想的简化实现
 */
public class AppendOnlyKVDB implements AutoCloseable {
    private final Database database;
    
    public AppendOnlyKVDB(String dataDir) throws IOException {
        this(dataDir, DatabaseConfig.defaultConfig());
    }
    
    public AppendOnlyKVDB(String dataDir, DatabaseConfig config) throws IOException {
        Path dataPath = Paths.get(dataDir);
        this.database = new Database(dataPath, config);
    }
    
    /**
     * 存储键值对
     */
    public void set(String key, String value) throws IOException {
        database.set(key, value);
    }
    
    /**
     * 根据键获取值
     */
    public String get(String key) throws IOException {
        return database.get(key);
    }
    
    /**
     * 删除键（逻辑删除，写入墓碑标记）
     */
    public void delete(String key) throws IOException {
        database.delete(key);
    }
    
    /**
     * 关闭数据库，停止后台服务
     */
    public void close() throws IOException {
        database.close();
    }
    
    /**
     * 手动触发Compaction
     */
    public void compact() throws IOException {
        database.compact();
    }
    
    /**
     * 获取数据库统计信息
     */
    public DatabaseStats getStats() {
        return database.getStats();
    }
    
    public static class DatabaseStats {
        public final int memTableSize;
        public final int sstableCount;
        public final long totalWrites;
        public final long totalReads;
        
        public DatabaseStats(int memTableSize, int sstableCount, long totalWrites, long totalReads) {
            this.memTableSize = memTableSize;
            this.sstableCount = sstableCount;
            this.totalWrites = totalWrites;
            this.totalReads = totalReads;
        }
        
        @Override
        public String toString() {
            return String.format("DatabaseStats{memTableSize=%d, sstableCount=%d, totalWrites=%d, totalReads=%d}",
                    memTableSize, sstableCount, totalWrites, totalReads);
        }
    }
}