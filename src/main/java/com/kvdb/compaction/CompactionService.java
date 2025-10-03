package com.kvdb.compaction;

import com.kvdb.core.DatabaseConfig;
import com.kvdb.core.KeyValue;
import com.kvdb.sstable.SSTableManager;
import com.kvdb.sstable.SSTableMetadata;
import com.kvdb.sstable.SSTableWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Size-Tiered Compaction服务
 * 负责后台执行SSTable合并操作
 */
public class CompactionService {
    private final SSTableManager sstableManager;
    private final DatabaseConfig config;
    private final ScheduledExecutorService scheduler;
    private final ReentrantLock compactionLock;
    private final AtomicBoolean running;
    private volatile boolean shutdown;
    
    public CompactionService(SSTableManager sstableManager, DatabaseConfig config) {
        this.sstableManager = sstableManager;
        this.config = config;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Compaction-Service");
            t.setDaemon(true);
            return t;
        });
        this.compactionLock = new ReentrantLock();
        this.running = new AtomicBoolean(false);
        this.shutdown = false;
    }
    
    /**
     * 启动Compaction服务
     */
    public void start() {
        if (!shutdown) {
            scheduler.scheduleWithFixedDelay(
                this::runCompaction,
                config.getCompactionIntervalMs(),
                config.getCompactionIntervalMs(),
                TimeUnit.MILLISECONDS
            );
        }
    }
    
    /**
     * 手动触发Compaction
     */
    public boolean triggerCompaction() {
        if (shutdown || !compactionLock.tryLock()) {
            return false;
        }
        
        try {
            return performCompaction();
        } finally {
            compactionLock.unlock();
        }
    }
    
    /**
     * 停止Compaction服务
     */
    public void shutdown() {
        shutdown = true;
        scheduler.shutdown();
        
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * 检查Compaction服务是否正在运行
     */
    public boolean isRunning() {
        return running.get();
    }
    
    /**
     * 定期执行的Compaction任务
     */
    private void runCompaction() {
        if (shutdown || !compactionLock.tryLock()) {
            return;
        }
        
        try {
            // 检查是否需要Compaction
            if (shouldTriggerCompaction()) {
                performCompaction();
            }
        } catch (Exception e) {
            System.err.println("Compaction error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            compactionLock.unlock();
        }
    }
    
    /**
     * 检查是否应该触发Compaction
     */
    private boolean shouldTriggerCompaction() {
        int sstableCount = sstableManager.getSSTableCount();
        return sstableCount >= config.getSstableCompactionThreshold();
    }
    
    /**
     * 执行实际的Compaction操作
     */
    private boolean performCompaction() {
        running.set(true);
        
        try {
            // 选择需要合并的SSTable
            List<SSTableMetadata> candidates = sstableManager.selectSSTablesForCompaction(
                config.getMaxCompactionFiles()
            );
            
            if (candidates.size() < 2) {
                return false; // 没有足够的文件需要合并
            }
            
            System.out.println("Starting compaction of " + candidates.size() + " SSTables");
            long startTime = System.currentTimeMillis();
            
            // 执行合并
            SSTableMetadata newSSTable = mergeSSTable(candidates);
            
            if (newSSTable != null) {
                // 添加新的SSTable
                sstableManager.addSSTable(newSSTable);
                
                // 移除旧的SSTable
                for (SSTableMetadata oldTable : candidates) {
                    sstableManager.removeSSTable(oldTable);
                }
                
                long duration = System.currentTimeMillis() - startTime;
                System.out.println(String.format(
                    "Compaction completed: %d files -> 1 file in %d ms (new file: %s)",
                    candidates.size(), duration, newSSTable.getFileName()
                ));
                
                return true;
            }
            
        } catch (IOException e) {
            System.err.println("Compaction failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            running.set(false);
        }
        
        return false;
    }
    
    /**
     * 合并多个SSTable为一个新的SSTable
     */
    private SSTableMetadata mergeSSTable(List<SSTableMetadata> sstables) throws IOException {
        // 生成新的SSTable文件路径
        Path newSSTablePath = sstableManager.generateSSTablePath();
        
        // 创建写入器
        SSTableWriter writer = new SSTableWriter(newSSTablePath);
        
        try (MergeIterator mergeIterator = new MergeIterator(sstables)) {
            
            int mergedEntries = 0;
            
            // 多路归并写入新文件
            while (mergeIterator.hasNext()) {
                KeyValue kv = mergeIterator.next();
                
                // 跳过墓碑标记（在Compaction过程中清理删除的数据）
                if (!kv.isTombstone()) {
                    writer.write(kv);
                    mergedEntries++;
                }
            }
            
            // 完成写入
            SSTableMetadata metadata = writer.finish();
            
            System.out.println("Merged " + mergedEntries + " entries into new SSTable: " + 
                              metadata.getFileName());
            
            return metadata;
            
        } catch (IOException e) {
            // 如果合并失败，取消写入操作
            writer.cancel();
            throw e;
        }
    }
    
    /**
     * 获取Compaction统计信息
     */
    public CompactionStats getStats() {
        return new CompactionStats(
            running.get(),
            sstableManager.getSSTableCount(),
            sstableManager.getTotalFileSize(),
            sstableManager.getTotalEntryCount()
        );
    }
    
    /**
     * Compaction统计信息
     */
    public static class CompactionStats {
        public final boolean isRunning;
        public final int sstableCount;
        public final long totalFileSize;
        public final long totalEntryCount;
        
        public CompactionStats(boolean isRunning, int sstableCount, 
                              long totalFileSize, long totalEntryCount) {
            this.isRunning = isRunning;
            this.sstableCount = sstableCount;
            this.totalFileSize = totalFileSize;
            this.totalEntryCount = totalEntryCount;
        }
        
        @Override
        public String toString() {
            return String.format(
                "CompactionStats{running=%s, sstables=%d, totalSize=%d bytes, totalEntries=%d}",
                isRunning, sstableCount, totalFileSize, totalEntryCount
            );
        }
    }
}