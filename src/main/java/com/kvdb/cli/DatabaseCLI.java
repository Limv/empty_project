package com.kvdb.cli;

import com.kvdb.AppendOnlyKVDB;
import com.kvdb.core.DatabaseConfig;

import java.io.IOException;
import java.util.Scanner;

/**
 * 数据库命令行工具
 * 提供简单的交互式接口进行数据库操作
 */
public class DatabaseCLI {
    private static final String DEFAULT_DATA_DIR = "./kvdb_data";
    
    public static void main(String[] args) {
        String dataDir = args.length > 0 ? args[0] : DEFAULT_DATA_DIR;
        
        System.out.println("=== Append-Only Key-Value Database CLI ===");
        System.out.println("Data directory: " + dataDir);
        System.out.println("Type 'help' for available commands");
        System.out.println();
        
        try (AppendOnlyKVDB db = new AppendOnlyKVDB(dataDir, createConfig());
             Scanner scanner = new Scanner(System.in)) {
            
            System.out.println("Database opened successfully!");
            
            while (true) {
                System.out.print("kvdb> ");
                String input = scanner.nextLine().trim();
                
                if (input.isEmpty()) {
                    continue;
                }
                
                try {
                    if (processCommand(db, input)) {
                        break; // exit命令
                    }
                } catch (Exception e) {
                    System.err.println("Error: " + e.getMessage());
                }
            }
            
        } catch (IOException e) {
            System.err.println("Failed to open database: " + e.getMessage());
            System.exit(1);
        }
        
        System.out.println("Database closed. Goodbye!");
    }
    
    /**
     * 处理用户命令
     * @return true表示退出程序
     */
    private static boolean processCommand(AppendOnlyKVDB db, String input) throws IOException {
        String[] parts = parseCommand(input);
        String command = parts[0].toLowerCase();
        
        switch (command) {
            case "help":
            case "h":
                printHelp();
                break;
                
            case "set":
            case "put":
                if (parts.length != 3) {
                    System.out.println("Usage: set <key> <value>");
                    break;
                }
                db.set(parts[1], parts[2]);
                System.out.println("OK");
                break;
                
            case "get":
                if (parts.length != 2) {
                    System.out.println("Usage: get <key>");
                    break;
                }
                String value = db.get(parts[1]);
                if (value != null) {
                    System.out.println(value);
                } else {
                    System.out.println("(null)");
                }
                break;
                
            case "delete":
            case "del":
                if (parts.length != 2) {
                    System.out.println("Usage: delete <key>");
                    break;
                }
                db.delete(parts[1]);
                System.out.println("OK");
                break;
                
            case "stats":
                System.out.println(db.getStats());
                break;
                
            case "compact":
                System.out.println("Triggering compaction...");
                db.compact();
                System.out.println("Compaction triggered");
                break;
                
            case "benchmark":
                if (parts.length >= 2) {
                    runBenchmark(db, parts);
                } else {
                    System.out.println("Usage: benchmark <operation> [count]");
                    System.out.println("Operations: write, read, mixed");
                }
                break;
                
            case "exit":
            case "quit":
            case "q":
                return true;
                
            default:
                System.out.println("Unknown command: " + command + ". Type 'help' for available commands.");
                break;
        }
        
        return false;
    }
    
    /**
     * 解析命令行输入
     */
    private static String[] parseCommand(String input) {
        // 简单的空格分割，不处理引号
        return input.split("\\s+");
    }
    
    /**
     * 打印帮助信息
     */
    private static void printHelp() {
        System.out.println("Available commands:");
        System.out.println("  set <key> <value>    - Set a key-value pair");
        System.out.println("  get <key>            - Get value by key");
        System.out.println("  delete <key>         - Delete a key");
        System.out.println("  stats                - Show database statistics");
        System.out.println("  compact              - Trigger manual compaction");
        System.out.println("  benchmark <op> [n]   - Run benchmark (write/read/mixed)");
        System.out.println("  help                 - Show this help");
        System.out.println("  exit                 - Exit the program");
    }
    
    /**
     * 运行性能测试
     */
    private static void runBenchmark(AppendOnlyKVDB db, String[] parts) throws IOException {
        String operation = parts[1].toLowerCase();
        int count = parts.length >= 3 ? Integer.parseInt(parts[2]) : 10000;
        
        System.out.println("Running " + operation + " benchmark with " + count + " operations...");
        
        long startTime = System.currentTimeMillis();
        
        switch (operation) {
            case "write":
                runWriteBenchmark(db, count);
                break;
            case "read":
                runReadBenchmark(db, count);
                break;
            case "mixed":
                runMixedBenchmark(db, count);
                break;
            default:
                System.out.println("Unknown benchmark operation: " + operation);
                return;
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        double opsPerSecond = (double) count / (duration / 1000.0);
        
        System.out.println(String.format("Benchmark completed: %d operations in %d ms (%.2f ops/sec)",
                count, duration, opsPerSecond));
        System.out.println("Final stats: " + db.getStats());
    }
    
    /**
     * 写入性能测试
     */
    private static void runWriteBenchmark(AppendOnlyKVDB db, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            String key = "key_" + i;
            String value = "value_" + i + "_" + System.currentTimeMillis();
            db.set(key, value);
            
            if (i % 1000 == 0) {
                System.out.print(".");
            }
        }
        System.out.println();
    }
    
    /**
     * 读取性能测试
     */
    private static void runReadBenchmark(AppendOnlyKVDB db, int count) throws IOException {
        // 先写入一些测试数据
        System.out.println("Preparing test data...");
        for (int i = 0; i < Math.min(count, 1000); i++) {
            db.set("test_key_" + i, "test_value_" + i);
        }
        
        // 执行读取测试
        for (int i = 0; i < count; i++) {
            String key = "test_key_" + (i % 1000);
            db.get(key);
            
            if (i % 1000 == 0) {
                System.out.print(".");
            }
        }
        System.out.println();
    }
    
    /**
     * 混合操作性能测试
     */
    private static void runMixedBenchmark(AppendOnlyKVDB db, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            if (i % 3 == 0) {
                // 读操作
                String key = "key_" + (i % 100);
                db.get(key);
            } else if (i % 7 == 0) {
                // 删除操作
                String key = "key_" + (i % 50);
                db.delete(key);
            } else {
                // 写操作
                String key = "key_" + i;
                String value = "value_" + i;
                db.set(key, value);
            }
            
            if (i % 1000 == 0) {
                System.out.print(".");
            }
        }
        System.out.println();
    }
    
    /**
     * 创建数据库配置
     */
    private static DatabaseConfig createConfig() {
        return DatabaseConfig.builder()
                .memTableMaxSize(5000)        // 较小的MemTable用于测试
                .memTableFlushThreshold(4000)
                .sstableCompactionThreshold(3) // 3个SSTable就触发Compaction
                .compactionIntervalMs(30000)   // 30秒检查一次
                .maxCompactionFiles(5)
                .enableWAL(true)
                .walSyncInterval(1000)
                .build();
    }
}