package com.kvdb.wal;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Write-Ahead Log (WAL) 实现
 * 确保写操作的持久性和崩溃恢复能力
 */
public class WriteAheadLog implements AutoCloseable {
    private final Path walFile;
    private BufferedOutputStream outputStream;
    private DataOutputStream dataStream;
    private final AtomicLong sequenceNumber;
    private final ReentrantLock writeLock;
    private final int syncInterval; // 同步间隔（毫秒）
    private long lastSyncTime;
    private boolean closed;
    
    public WriteAheadLog(Path walFile, int syncInterval) throws IOException {
        this.walFile = walFile;
        this.syncInterval = syncInterval;
        this.sequenceNumber = new AtomicLong(0);
        this.writeLock = new ReentrantLock();
        this.lastSyncTime = System.currentTimeMillis();
        this.closed = false;
        
        // 创建WAL文件的父目录
        Files.createDirectories(walFile.getParent());
        
        // 初始化流对象
        initializeStreams();
        
        // 如果WAL文件已存在，需要读取最后的序列号
        if (Files.exists(walFile) && Files.size(walFile) > 0) {
            loadLastSequenceNumber();
        }
    }
    
    /**
     * 初始化流对象
     */
    private void initializeStreams() throws IOException {
        this.outputStream = new BufferedOutputStream(
            Files.newOutputStream(walFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
        );
        this.dataStream = new DataOutputStream(outputStream);
    }
    
    /**
     * 记录PUT操作到WAL
     */
    public long logPut(String key, String value) throws IOException {
        if (closed) {
            throw new IllegalStateException("WAL is closed");
        }
        
        long seqNum = sequenceNumber.incrementAndGet();
        WALRecord record = WALRecord.createPut(key, value, seqNum);
        
        writeLock.lock();
        try {
            WALSerializer.writeRecord(dataStream, record);
            
            // 根据同步策略决定是否立即刷盘
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastSyncTime >= syncInterval) {
                sync();
                lastSyncTime = currentTime;
            }
        } finally {
            writeLock.unlock();
        }
        
        return seqNum;
    }
    
    /**
     * 记录DELETE操作到WAL
     */
    public long logDelete(String key) throws IOException {
        if (closed) {
            throw new IllegalStateException("WAL is closed");
        }
        
        long seqNum = sequenceNumber.incrementAndGet();
        WALRecord record = WALRecord.createDelete(key, seqNum);
        
        writeLock.lock();
        try {
            WALSerializer.writeRecord(dataStream, record);
            
            // 根据同步策略决定是否立即刷盘
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastSyncTime >= syncInterval) {
                sync();
                lastSyncTime = currentTime;
            }
        } finally {
            writeLock.unlock();
        }
        
        return seqNum;
    }
    
    /**
     * 强制将缓冲区的数据刷盘
     */
    public void sync() throws IOException {
        if (closed) {
            return;
        }
        
        writeLock.lock();
        try {
            dataStream.flush();
            outputStream.flush();
            // 注意：这里可能需要调用FileDescriptor.sync()来确保OS缓冲区也被刷盘
            // 但在Java 8中，这需要使用RandomAccessFile或FileChannel
        } finally {
            writeLock.unlock();
        }
    }
    
    /**
     * 恢复WAL中的所有记录
     */
    public List<WALRecord> recover() throws IOException {
        List<WALRecord> records = new ArrayList<>();
        
        if (!Files.exists(walFile)) {
            return records; // WAL文件不存在，没有需要恢复的数据
        }
        
        try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(walFile));
             DataInputStream dis = new DataInputStream(bis)) {
            
            while (dis.available() > 0) {
                try {
                    WALRecord record = WALSerializer.readRecord(dis);
                    records.add(record);
                    
                    // 更新序列号
                    long currentSeq = sequenceNumber.get();
                    if (record.getSequenceNumber() > currentSeq) {
                        sequenceNumber.set(record.getSequenceNumber());
                    }
                } catch (EOFException e) {
                    // 到达文件末尾
                    break;
                } catch (IOException e) {
                    // 记录损坏，记录警告但继续处理
                    System.err.println("Warning: Found corrupted WAL record, skipping: " + e.getMessage());
                    break;
                }
            }
        }
        
        return records;
    }
    
    /**
     * 清除WAL文件内容
     */
    public void clear() throws IOException {
        writeLock.lock();
        try {
            // 先关闭当前的流
            if (dataStream != null) {
                dataStream.close();
            }
            if (outputStream != null) {
                outputStream.close();
            }
            
            // 删除WAL文件
            Files.deleteIfExists(walFile);
            
            // 重新初始化流对象
            initializeStreams();
            
            closed = false;
            lastSyncTime = System.currentTimeMillis();
        } finally {
            writeLock.unlock();
        }
    }
    
    /**
     * 获取当前序列号
     */
    public long getCurrentSequenceNumber() {
        return sequenceNumber.get();
    }
    
    /**
     * 获取WAL文件大小
     */
    public long getFileSize() throws IOException {
        return Files.exists(walFile) ? Files.size(walFile) : 0;
    }
    
    /**
     * 检查WAL是否已关闭
     */
    public boolean isClosed() {
        return closed;
    }
    
    /**
     * 从现有WAL文件中加载最后的序列号
     */
    private void loadLastSequenceNumber() throws IOException {
        long maxSeq = 0;
        
        try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(walFile));
             DataInputStream dis = new DataInputStream(bis)) {
            
            while (dis.available() > 0) {
                try {
                    WALRecord record = WALSerializer.readRecord(dis);
                    if (record.getSequenceNumber() > maxSeq) {
                        maxSeq = record.getSequenceNumber();
                    }
                } catch (EOFException e) {
                    break;
                } catch (IOException e) {
                    // 忽略损坏的记录
                    break;
                }
            }
        }
        
        sequenceNumber.set(maxSeq);
    }
    
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        
        writeLock.lock();
        try {
            closed = true;
            sync(); // 最后一次刷盘
            dataStream.close();
            outputStream.close();
        } finally {
            writeLock.unlock();
        }
    }
    
    @Override
    public String toString() {
        return String.format("WriteAheadLog{file='%s', sequenceNumber=%d, closed=%s}",
                walFile.getFileName(), sequenceNumber.get(), closed);
    }
}