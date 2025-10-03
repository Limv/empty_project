package com.kvdb.sstable;

import com.kvdb.core.KeyValue;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * SSTable文件序列化/反序列化工具
 * 负责键值对的二进制格式读写
 */
public class SSTableSerializer {
    
    /**
     * 将KeyValue序列化为字节数组
     * 格式: [keyLen:4][key:keyLen][valueLen:4][value:valueLen][tombstone:1][timestamp:8]
     */
    public static byte[] serialize(KeyValue kv) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        // 写入key
        byte[] keyBytes = kv.getKey().getBytes(StandardCharsets.UTF_8);
        dos.writeInt(keyBytes.length);
        dos.write(keyBytes);
        
        // 写入value
        if (kv.getValue() != null) {
            byte[] valueBytes = kv.getValue().getBytes(StandardCharsets.UTF_8);
            dos.writeInt(valueBytes.length);
            dos.write(valueBytes);
        } else {
            dos.writeInt(0); // null value的长度为0
        }
        
        // 写入tombstone标记
        dos.writeBoolean(kv.isTombstone());
        
        // 写入时间戳
        dos.writeLong(kv.getTimestamp());
        
        dos.close();
        return baos.toByteArray();
    }
    
    /**
     * 从字节数组反序列化KeyValue
     */
    public static KeyValue deserialize(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        
        // 读取key
        int keyLen = dis.readInt();
        byte[] keyBytes = new byte[keyLen];
        dis.readFully(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);
        
        // 读取value
        int valueLen = dis.readInt();
        String value = null;
        if (valueLen > 0) {
            byte[] valueBytes = new byte[valueLen];
            dis.readFully(valueBytes);
            value = new String(valueBytes, StandardCharsets.UTF_8);
        }
        
        // 读取tombstone标记
        boolean tombstone = dis.readBoolean();
        
        // 读取时间戳
        long timestamp = dis.readLong();
        
        dis.close();
        return new KeyValue(key, value, tombstone, timestamp);
    }
    
    /**
     * 序列化索引项
     * 格式: [keyLen:4][key:keyLen][offset:8]
     */
    public static byte[] serializeIndexEntry(String key, long offset) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(keyBytes.length);
        dos.write(keyBytes);
        dos.writeLong(offset);
        
        dos.close();
        return baos.toByteArray();
    }
    
    /**
     * 反序列化索引项
     */
    public static IndexEntry deserializeIndexEntry(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        
        int keyLen = dis.readInt();
        byte[] keyBytes = new byte[keyLen];
        dis.readFully(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);
        
        long offset = dis.readLong();
        
        dis.close();
        return new IndexEntry(key, offset);
    }
    
    /**
     * 从InputStream读取一个完整的KeyValue
     */
    public static KeyValue readKeyValue(DataInputStream dis) throws IOException {
        // 读取key
        int keyLen = dis.readInt();
        byte[] keyBytes = new byte[keyLen];
        dis.readFully(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);
        
        // 读取value
        int valueLen = dis.readInt();
        String value = null;
        if (valueLen > 0) {
            byte[] valueBytes = new byte[valueLen];
            dis.readFully(valueBytes);
            value = new String(valueBytes, StandardCharsets.UTF_8);
        }
        
        // 读取tombstone标记
        boolean tombstone = dis.readBoolean();
        
        // 读取时间戳
        long timestamp = dis.readLong();
        
        return new KeyValue(key, value, tombstone, timestamp);
    }
    
    /**
     * 向OutputStream写入一个KeyValue
     */
    public static void writeKeyValue(DataOutputStream dos, KeyValue kv) throws IOException {
        // 写入key
        byte[] keyBytes = kv.getKey().getBytes(StandardCharsets.UTF_8);
        dos.writeInt(keyBytes.length);
        dos.write(keyBytes);
        
        // 写入value
        if (kv.getValue() != null) {
            byte[] valueBytes = kv.getValue().getBytes(StandardCharsets.UTF_8);
            dos.writeInt(valueBytes.length);
            dos.write(valueBytes);
        } else {
            dos.writeInt(0);
        }
        
        // 写入tombstone标记
        dos.writeBoolean(kv.isTombstone());
        
        // 写入时间戳
        dos.writeLong(kv.getTimestamp());
    }
    
    /**
     * 读取所有索引项
     */
    public static List<IndexEntry> readAllIndexEntries(DataInputStream dis) throws IOException {
        List<IndexEntry> entries = new ArrayList<>();
        
        try {
            while (true) {
                int keyLen = dis.readInt();
                byte[] keyBytes = new byte[keyLen];
                dis.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);
                
                long offset = dis.readLong();
                entries.add(new IndexEntry(key, offset));
            }
        } catch (EOFException e) {
            // 到达文件末尾是正常的
        }
        
        return entries;
    }
    
    /**
     * 索引项内部类
     */
    public static class IndexEntry {
        private final String key;
        private final long offset;
        
        public IndexEntry(String key, long offset) {
            this.key = key;
            this.offset = offset;
        }
        
        public String getKey() {
            return key;
        }
        
        public long getOffset() {
            return offset;
        }
        
        @Override
        public String toString() {
            return String.format("IndexEntry{key='%s', offset=%d}", key, offset);
        }
    }
}