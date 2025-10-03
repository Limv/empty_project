package com.kvdb.core;

import com.kvdb.AppendOnlyKVDB;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

/**
 * Database核心功能测试
 */
public class DatabaseTest {
    private Path testDataDir;
    private AppendOnlyKVDB database;
    
    @Before
    public void setUp() throws IOException {
        // 创建临时测试目录
        testDataDir = Files.createTempDirectory("kvdb_test_");
        
        // 创建测试配置
        DatabaseConfig config = DatabaseConfig.builder()
                .memTableMaxSize(100)
                .memTableFlushThreshold(80)
                .sstableCompactionThreshold(3)
                .compactionIntervalMs(1000)
                .enableWAL(true)
                .build();
        
        database = new AppendOnlyKVDB(testDataDir.toString(), config);
    }
    
    @After
    public void tearDown() throws IOException {
        if (database != null) {
            database.close();
        }
        
        // 清理测试目录
        deleteDirectory(testDataDir);
    }
    
    @Test
    public void testBasicSetAndGet() throws IOException {
        // 测试基本的set和get操作
        database.set("key1", "value1");
        database.set("key2", "value2");
        
        assertEquals("value1", database.get("key1"));
        assertEquals("value2", database.get("key2"));
        assertNull(database.get("nonexistent"));
    }
    
    @Test
    public void testUpdate() throws IOException {
        // 测试更新操作
        database.set("key1", "value1");
        assertEquals("value1", database.get("key1"));
        
        database.set("key1", "value2");
        assertEquals("value2", database.get("key1"));
    }
    
    @Test
    public void testDelete() throws IOException {
        // 测试删除操作
        database.set("key1", "value1");
        assertEquals("value1", database.get("key1"));
        
        database.delete("key1");
        assertNull(database.get("key1"));
    }
    
    @Test
    public void testMemTableFlush() throws IOException {
        // 测试MemTable刷盘
        for (int i = 0; i < 150; i++) {
            database.set("key" + i, "value" + i);
        }
        
        // 等待刷盘完成
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // 验证数据仍然可以读取
        for (int i = 0; i < 150; i++) {
            assertEquals("value" + i, database.get("key" + i));
        }
    }
    
    @Test
    public void testLargeDataSet() throws IOException {
        // 测试大量数据
        int count = 1000;
        
        // 写入数据
        for (int i = 0; i < count; i++) {
            database.set("key" + i, "value" + i);
        }
        
        // 读取数据验证
        for (int i = 0; i < count; i++) {
            assertEquals("value" + i, database.get("key" + i));
        }
        
        // 删除部分数据
        for (int i = 0; i < count; i += 2) {
            database.delete("key" + i);
        }
        
        // 验证删除结果
        for (int i = 0; i < count; i++) {
            if (i % 2 == 0) {
                assertNull(database.get("key" + i));
            } else {
                assertEquals("value" + i, database.get("key" + i));
            }
        }
    }
    
    @Test
    public void testStats() throws IOException {
        database.set("key1", "value1");
        database.set("key2", "value2");
        database.get("key1");
        
        AppendOnlyKVDB.DatabaseStats stats = database.getStats();
        assertTrue(stats.totalWrites >= 2);
        assertTrue(stats.totalReads >= 1);
        assertTrue(stats.memTableSize >= 0);
    }
    
    /**
     * 递归删除目录
     */
    private void deleteDirectory(Path directory) throws IOException {
        if (Files.exists(directory)) {
            Files.walk(directory)
                    .sorted((p1, p2) -> p2.compareTo(p1)) // 逆序，先删除文件再删除目录
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // 忽略删除错误
                        }
                    });
        }
    }
}