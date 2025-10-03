package com.kvdb.memtable;

import com.kvdb.core.KeyValue;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * MemTable测试
 */
public class MemTableTest {
    private MemTable memTable;
    
    @Before
    public void setUp() {
        memTable = new MemTable();
    }
    
    @Test
    public void testBasicOperations() {
        // 测试基本插入和查询
        memTable.put("key1", "value1");
        memTable.put("key2", "value2");
        
        assertEquals("value1", memTable.get("key1").getValue());
        assertEquals("value2", memTable.get("key2").getValue());
        assertNull(memTable.get("nonexistent"));
        
        assertEquals(2, memTable.size());
        assertFalse(memTable.isEmpty());
    }
    
    @Test
    public void testUpdate() {
        // 测试更新操作
        memTable.put("key1", "value1");
        assertEquals("value1", memTable.get("key1").getValue());
        
        memTable.put("key1", "value2");
        assertEquals("value2", memTable.get("key1").getValue());
        assertEquals(1, memTable.size()); // 大小不应该增加
    }
    
    @Test
    public void testDelete() {
        // 测试删除操作
        memTable.put("key1", "value1");
        memTable.delete("key1");
        
        KeyValue kv = memTable.get("key1");
        assertNotNull(kv);
        assertTrue(kv.isTombstone());
        assertEquals(1, memTable.size());
    }
    
    @Test
    public void testOrdering() {
        // 测试键的排序
        memTable.put("key3", "value3");
        memTable.put("key1", "value1");
        memTable.put("key2", "value2");
        
        Iterator<KeyValue> iterator = memTable.iterator();
        
        assertTrue(iterator.hasNext());
        assertEquals("key1", iterator.next().getKey());
        
        assertTrue(iterator.hasNext());
        assertEquals("key2", iterator.next().getKey());
        
        assertTrue(iterator.hasNext());
        assertEquals("key3", iterator.next().getKey());
        
        assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testSnapshot() {
        memTable.put("key1", "value1");
        memTable.put("key2", "value2");
        
        MemTableSnapshot snapshot = memTable.createSnapshot();
        
        assertEquals("value1", snapshot.get("key1").getValue());
        assertEquals("value2", snapshot.get("key2").getValue());
        assertEquals(2, snapshot.size());
        
        // 修改原MemTable不应该影响快照
        memTable.put("key3", "value3");
        assertNull(snapshot.get("key3"));
        assertEquals(2, snapshot.size());
    }
    
    @Test
    public void testMemoryUsage() {
        long initialUsage = memTable.getMemoryUsage();
        
        memTable.put("key1", "value1");
        assertTrue(memTable.getMemoryUsage() > initialUsage);
        
        long usage1 = memTable.getMemoryUsage();
        memTable.put("key2", "value2");
        assertTrue(memTable.getMemoryUsage() > usage1);
    }
    
    @Test
    public void testClear() {
        memTable.put("key1", "value1");
        memTable.put("key2", "value2");
        
        assertFalse(memTable.isEmpty());
        
        memTable.clear();
        assertTrue(memTable.isEmpty());
        assertEquals(0, memTable.size());
        assertNull(memTable.get("key1"));
    }
}