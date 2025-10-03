# Append-Only Key-Value Database

基于LSM-Tree思想的简化键值存储数据库实现，使用Java 8开发。

## 项目概述

这是一个教育性质的数据库项目，实现了LSM-Tree的核心概念：
- **Append-Only写入**：所有写操作都是顺序追加，避免随机I/O
- **分层存储**：MemTable（内存）+ SSTable（磁盘）的分层架构
- **后台合并**：Size-Tiered Compaction策略，定期合并SSTable文件
- **崩溃恢复**：WAL（Write-Ahead Log）机制确保数据持久性

## 架构特性

### 核心组件
- **MemTable**：基于ConcurrentSkipListMap的内存表，保证线程安全和有序性
- **SSTable**：排序的不可变磁盘文件，包含数据区、索引区和元数据
- **WAL**：预写日志，确保写操作的持久性
- **Compaction Service**：后台合并服务，实现Size-Tiered策略

### 关键特性
- ✅ 高写入吞吐量（顺序写入）
- ✅ 支持基本的Get/Set/Delete操作
- ✅ 逻辑删除（Tombstone标记）
- ✅ 自动MemTable刷盘
- ✅ 后台SSTable合并
- ✅ 崩溃恢复机制
- ✅ 线程安全的并发操作

## 快速开始

### 环境要求
- Java 8+
- Maven 3.6+

### 编译和运行

```bash
# 编译项目
mvn clean compile

# 运行测试
mvn test

# 打包
mvn package

# 运行CLI工具
java -cp target/appendonly-kvdb-1.0.0.jar com.kvdb.cli.DatabaseCLI [data_directory]
```

### 使用示例

```java
// 创建数据库实例
AppendOnlyKVDB db = new AppendOnlyKVDB("./data");

// 基本操作
db.set("key1", "value1");
String value = db.get("key1");
db.delete("key1");

// 查看统计信息
DatabaseStats stats = db.getStats();
System.out.println(stats);

// 手动触发Compaction
db.compact();

// 关闭数据库
db.close();
```

### CLI命令

```bash
kvdb> set key1 value1    # 设置键值对
kvdb> get key1           # 获取值
kvdb> delete key1        # 删除键
kvdb> stats              # 显示统计信息
kvdb> compact            # 手动触发Compaction
kvdb> benchmark write 1000  # 性能测试
kvdb> help               # 显示帮助
kvdb> exit               # 退出
```

## 项目结构

```
src/main/java/com/kvdb/
├── AppendOnlyKVDB.java          # 主要API入口
├── core/
│   ├── Database.java            # 数据库核心实现
│   ├── DatabaseConfig.java     # 配置类
│   └── KeyValue.java           # 键值对数据结构
├── memtable/
│   ├── MemTable.java           # 内存表实现
│   └── MemTableSnapshot.java   # 内存表快照
├── sstable/
│   ├── SSTableManager.java     # SSTable管理器
│   ├── SSTableReader.java      # SSTable读取器
│   ├── SSTableWriter.java      # SSTable写入器
│   ├── SSTableIterator.java    # SSTable迭代器
│   ├── SSTableMetadata.java    # SSTable元数据
│   └── SSTableSerializer.java  # 序列化工具
├── wal/
│   ├── WriteAheadLog.java      # WAL实现
│   ├── WALRecord.java          # WAL记录
│   └── WALSerializer.java      # WAL序列化
├── compaction/
│   ├── CompactionService.java  # Compaction服务
│   └── MergeIterator.java      # 多路归并迭代器
└── cli/
    └── DatabaseCLI.java        # 命令行工具
```

## 技术实现

### 数据流程

#### 写入流程（Set操作）
1. 写入WAL确保持久性
2. 写入活跃MemTable
3. 检查MemTable大小，必要时触发刷盘
4. 后台异步将不可变MemTable写入SSTable

#### 读取流程（Get操作）
1. 查找活跃MemTable
2. 查找不可变MemTable（如果存在）
3. 按时间倒序查找SSTable文件
4. 使用索引和范围检查优化查找

#### Compaction流程
1. 后台服务定期检查SSTable数量
2. 选择Size-Tiered策略中的候选文件
3. 多路归并排序合并数据
4. 生成新的SSTable文件
5. 更新元数据并删除旧文件

### 文件格式

#### SSTable格式
```
[数据区: KeyValue序列]
[索引区: Key->Offset映射]
[索引偏移量: 8字节]
[元数据区: 文件信息]
```

#### WAL格式
```
[记录类型: 1字节][序列号: 8字节][时间戳: 8字节]
[Key长度: 4字节][Key数据][Value长度: 4字节][Value数据]
```

## 性能特点

### 优势
- **高写入性能**：顺序写入，避免随机I/O
- **良好的范围查询**：SSTable内部有序存储
- **空间效率**：Compaction过程中清理过期数据

### 限制
- **读放大**：可能需要查找多个SSTable
- **写放大**：Compaction带来额外的写入开销
- **临时空间占用**：Compaction过程中需要额外磁盘空间

## 配置参数

```java
DatabaseConfig config = DatabaseConfig.builder()
    .memTableMaxSize(10000)              // MemTable最大条目数
    .memTableFlushThreshold(8000)        // 刷盘阈值
    .sstableCompactionThreshold(4)       // 触发Compaction的SSTable数量
    .compactionIntervalMs(60000)         // Compaction检查间隔
    .maxCompactionFiles(10)              // 单次Compaction最大文件数
    .enableWAL(true)                     // 是否启用WAL
    .walSyncInterval(1000)               // WAL同步间隔
    .build();
```

## 测试

运行完整的测试套件：
```bash
mvn test
```

性能测试：
```bash
# 编译后运行CLI工具
java -cp target/classes com.kvdb.cli.DatabaseCLI

# 在CLI中运行性能测试
kvdb> benchmark write 10000    # 写入测试
kvdb> benchmark read 10000     # 读取测试
kvdb> benchmark mixed 10000    # 混合测试
```

## 未来改进方向

### 功能增强
- [ ] 实现布隆过滤器优化读取性能
- [ ] 支持范围查询和迭代器
- [ ] 实现快照和备份机制
- [ ] 添加数据压缩支持

### 性能优化
- [ ] 实现Leveled Compaction策略
- [ ] 添加Block Cache缓存
- [ ] 并行Compaction支持
- [ ] 零拷贝优化

### 工程完善
- [ ] 完善错误处理和恢复机制
- [ ] 添加监控和日志系统
- [ ] 实现配置热更新
- [ ] 性能监控和调试工具

## 学习价值

这个项目展示了：
1. **LSM-Tree存储引擎**的核心概念和实现
2. **Java并发编程**的实际应用
3. **文件I/O和序列化**的高效处理
4. **系统设计和架构**的思考过程
5. **数据库内核**的基本原理

## 许可证

MIT License - 仅用于教育和学习目的

## 作者

基于LSM-Tree论文和现代数据库系统设计理念实现的教育项目。