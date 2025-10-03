package com.kvdb.core;

/**
 * 数据库配置类
 */
public class DatabaseConfig {
    // MemTable相关配置
    private final int memTableMaxSize;           // MemTable最大大小（条目数）
    private final int memTableFlushThreshold;    // 刷盘阈值
    
    // Compaction相关配置
    private final int sstableCompactionThreshold;  // 触发Compaction的SSTable数量阈值
    private final long compactionIntervalMs;       // Compaction检查间隔（毫秒）
    private final int maxCompactionFiles;          // 单次Compaction最大文件数
    
    // 文件相关配置
    private final int bufferSize;                   // I/O缓冲区大小
    private final boolean enableBloomFilter;       // 是否启用布隆过滤器
    private final double bloomFilterFpp;           // 布隆过滤器误报率
    
    // WAL相关配置
    private final boolean enableWAL;               // 是否启用WAL
    private final int walSyncInterval;             // WAL同步间隔
    
    private DatabaseConfig(Builder builder) {
        this.memTableMaxSize = builder.memTableMaxSize;
        this.memTableFlushThreshold = builder.memTableFlushThreshold;
        this.sstableCompactionThreshold = builder.sstableCompactionThreshold;
        this.compactionIntervalMs = builder.compactionIntervalMs;
        this.maxCompactionFiles = builder.maxCompactionFiles;
        this.bufferSize = builder.bufferSize;
        this.enableBloomFilter = builder.enableBloomFilter;
        this.bloomFilterFpp = builder.bloomFilterFpp;
        this.enableWAL = builder.enableWAL;
        this.walSyncInterval = builder.walSyncInterval;
    }
    
    public static DatabaseConfig defaultConfig() {
        return new Builder().build();
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    // Getters
    public int getMemTableMaxSize() { return memTableMaxSize; }
    public int getMemTableFlushThreshold() { return memTableFlushThreshold; }
    public int getSstableCompactionThreshold() { return sstableCompactionThreshold; }
    public long getCompactionIntervalMs() { return compactionIntervalMs; }
    public int getMaxCompactionFiles() { return maxCompactionFiles; }
    public int getBufferSize() { return bufferSize; }
    public boolean isEnableBloomFilter() { return enableBloomFilter; }
    public double getBloomFilterFpp() { return bloomFilterFpp; }
    public boolean isEnableWAL() { return enableWAL; }
    public int getWalSyncInterval() { return walSyncInterval; }
    
    public static class Builder {
        private int memTableMaxSize = 10000;
        private int memTableFlushThreshold = 8000;
        private int sstableCompactionThreshold = 4;
        private long compactionIntervalMs = 60000; // 1分钟
        private int maxCompactionFiles = 10;
        private int bufferSize = 64 * 1024; // 64KB
        private boolean enableBloomFilter = true;
        private double bloomFilterFpp = 0.01; // 1%误报率
        private boolean enableWAL = true;
        private int walSyncInterval = 1000; // 1秒
        
        public Builder memTableMaxSize(int size) {
            this.memTableMaxSize = size;
            return this;
        }
        
        public Builder memTableFlushThreshold(int threshold) {
            this.memTableFlushThreshold = threshold;
            return this;
        }
        
        public Builder sstableCompactionThreshold(int threshold) {
            this.sstableCompactionThreshold = threshold;
            return this;
        }
        
        public Builder compactionIntervalMs(long intervalMs) {
            this.compactionIntervalMs = intervalMs;
            return this;
        }
        
        public Builder maxCompactionFiles(int maxFiles) {
            this.maxCompactionFiles = maxFiles;
            return this;
        }
        
        public Builder bufferSize(int size) {
            this.bufferSize = size;
            return this;
        }
        
        public Builder enableBloomFilter(boolean enable) {
            this.enableBloomFilter = enable;
            return this;
        }
        
        public Builder bloomFilterFpp(double fpp) {
            this.bloomFilterFpp = fpp;
            return this;
        }
        
        public Builder enableWAL(boolean enable) {
            this.enableWAL = enable;
            return this;
        }
        
        public Builder walSyncInterval(int interval) {
            this.walSyncInterval = interval;
            return this;
        }
        
        public DatabaseConfig build() {
            return new DatabaseConfig(this);
        }
    }
}