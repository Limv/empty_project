package com.kvdb.wal;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * WAL序列化器
 * 负责WAL记录的序列化和反序列化
 */
public class WALSerializer {
    
    /**
     * 序列化WAL记录
     * 格式: [type:1][sequenceNumber:8][timestamp:8][keyLen:4][key:keyLen][valueLen:4][value:valueLen]
     */
    public static byte[] serialize(WALRecord record) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        // 写入记录类型
        dos.writeByte(record.getType().getValue());
        
        // 写入序列号
        dos.writeLong(record.getSequenceNumber());
        
        // 写入时间戳
        dos.writeLong(record.getTimestamp());
        
        // 写入key
        byte[] keyBytes = record.getKey().getBytes(StandardCharsets.UTF_8);
        dos.writeInt(keyBytes.length);
        dos.write(keyBytes);
        
        // 写入value（如果存在）
        if (record.getValue() != null) {
            byte[] valueBytes = record.getValue().getBytes(StandardCharsets.UTF_8);
            dos.writeInt(valueBytes.length);
            dos.write(valueBytes);
        } else {
            dos.writeInt(0); // value长度为0表示null
        }
        
        dos.close();
        return baos.toByteArray();
    }
    
    /**
     * 反序列化WAL记录
     */
    public static WALRecord deserialize(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        
        // 读取记录类型
        WALRecordType type = WALRecordType.fromByte(dis.readByte());
        
        // 读取序列号
        long sequenceNumber = dis.readLong();
        
        // 读取时间戳
        long timestamp = dis.readLong();
        
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
        
        dis.close();
        return new WALRecord(type, key, value, timestamp, sequenceNumber);
    }
    
    /**
     * 从InputStream读取WAL记录
     */
    public static WALRecord readRecord(DataInputStream dis) throws IOException {
        // 读取记录类型
        WALRecordType type = WALRecordType.fromByte(dis.readByte());
        
        // 读取序列号
        long sequenceNumber = dis.readLong();
        
        // 读取时间戳
        long timestamp = dis.readLong();
        
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
        
        return new WALRecord(type, key, value, timestamp, sequenceNumber);
    }
    
    /**
     * 向OutputStream写入WAL记录
     */
    public static void writeRecord(DataOutputStream dos, WALRecord record) throws IOException {
        // 写入记录类型
        dos.writeByte(record.getType().getValue());
        
        // 写入序列号
        dos.writeLong(record.getSequenceNumber());
        
        // 写入时间戳
        dos.writeLong(record.getTimestamp());
        
        // 写入key
        byte[] keyBytes = record.getKey().getBytes(StandardCharsets.UTF_8);
        dos.writeInt(keyBytes.length);
        dos.write(keyBytes);
        
        // 写入value（如果存在）
        if (record.getValue() != null) {
            byte[] valueBytes = record.getValue().getBytes(StandardCharsets.UTF_8);
            dos.writeInt(valueBytes.length);
            dos.write(valueBytes);
        } else {
            dos.writeInt(0);
        }
    }
}