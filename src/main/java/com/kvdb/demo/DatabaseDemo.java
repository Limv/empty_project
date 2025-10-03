package com.kvdb.demo;

import com.kvdb.AppendOnlyKVDB;
import com.kvdb.core.DatabaseConfig;

import java.io.IOException;

/**
 * 数据库演示程序
 */
public class DatabaseDemo {
    public static void main(String[] args) {
        String dataDir = args.length > 0 ? args[0] : "./demo_data";
        
        System.out.println("=== Append-Only Key-Value Database Demo ===");
        System.out.println("Data directory: " + dataDir);
        System.out.println();
        
        try {
            // 创建数据库配置
            DatabaseConfig config = DatabaseConfig.builder()
                    .memTableMaxSize(1000)
                    .memTableFlushThreshold(800)
                    .sstableCompactionThreshold(3)
                    .compactionIntervalMs(10000) // 10秒
                    .enableWAL(true)
                    .build();
            
            // 创建数据库实例
            try (AppendOnlyKVDB db = new AppendOnlyKVDB(dataDir, config)) {
                System.out.println("Database opened successfully!");
                
                // 演示基本操作
                demonstrateBasicOperations(db);
                
                // 演示大量数据写入和MemTable刷盘
                demonstrateMemTableFlush(db);
                
                // 演示删除操作
                demonstrateDeletion(db);
                
                // 显示最终统计信息
                System.out.println("\\nFinal database stats:");
                System.out.println(db.getStats());
                
            }
            
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println("\\nDemo completed!");
    }
    
    private static void demonstrateBasicOperations(AppendOnlyKVDB db) throws IOException {
        System.out.println("1. Basic Operations Demo");
        System.out.println("========================");
        
        // 写入一些键值对
        db.set("user:1", "Alice");
        db.set("user:2", "Bob");
        db.set("user:3", "Charlie");
        System.out.println("Written 3 user records");
        
        // 读取数据
        System.out.println("user:1 = " + db.get("user:1"));
        System.out.println("user:2 = " + db.get("user:2"));
        System.out.println("user:3 = " + db.get("user:3"));
        System.out.println("user:999 = " + db.get("user:999"));
        
        // 更新数据
        db.set("user:1", "Alice Smith");
        System.out.println("Updated user:1 = " + db.get("user:1"));
        
        System.out.println("Stats: " + db.getStats());
        System.out.println();
    }
    
    private static void demonstrateMemTableFlush(AppendOnlyKVDB db) throws IOException {
        System.out.println("2. MemTable Flush Demo");
        System.out.println("======================");
        
        System.out.println("Writing 1500 records to trigger MemTable flush...");
        
        // 写入大量数据触发MemTable刷盘
        for (int i = 0; i < 1500; i++) {
            String key = "data:" + String.format("%04d", i);
            String value = "value_" + i + "_" + System.currentTimeMillis();
            db.set(key, value);
            
            if (i % 200 == 0) {
                System.out.print(".");
            }
        }
        System.out.println();
        
        // 等待刷盘完成
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // 验证数据仍然可读
        System.out.println("Verifying some records:");
        System.out.println("data:0000 = " + db.get("data:0000"));
        System.out.println("data:0500 = " + db.get("data:0500"));
        System.out.println("data:1000 = " + db.get("data:1000"));
        
        System.out.println("Stats after flush: " + db.getStats());
        System.out.println();
    }
    
    private static void demonstrateDeletion(AppendOnlyKVDB db) throws IOException {
        System.out.println("3. Deletion Demo");
        System.out.println("================");
        
        // 删除一些数据
        db.delete("user:2");
        db.delete("data:0100");
        db.delete("data:0200");
        System.out.println("Deleted some records");
        
        // 验证删除
        System.out.println("user:1 = " + db.get("user:1"));
        System.out.println("user:2 = " + db.get("user:2")); // 应该是null
        System.out.println("user:3 = " + db.get("user:3"));
        System.out.println("data:0100 = " + db.get("data:0100")); // 应该是null
        
        System.out.println("Stats after deletion: " + db.getStats());
        System.out.println();
    }
}