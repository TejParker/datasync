# MongoDB 到 StarRocks 数据同步

本项目基于 Flink CDC 实现 MongoDB 数据到 StarRocks 的实时同步。

## 功能特点

- 使用 Flink CDC 读取 MongoDB 数据变更
- 支持实时捕获 MongoDB 的插入、更新和删除操作
- 通过 HTTP Stream Load 方式写入 StarRocks
- 使用删除标记字段实现逻辑删除，避免物化节点内存溢出问题

## 环境要求

- Java 17+
- Apache Flink 1.17.2+
- MongoDB
- StarRocks

## 配置说明

在 `src/main/resources/application.properties` 文件中配置以下参数：

```properties
# MongoDB配置
mongodb.hosts=127.0.0.1:27017
mongodb.username=your_username
mongodb.password=your_password
mongodb.database=your_database
mongodb.collection=your_collection

# StarRocks配置
starrocks.host=localhost
starrocks.port=8030
starrocks.database=your_database
starrocks.table=your_table
starrocks.user=root
starrocks.password=

# Flink配置
flink.parallelism=2
```

## 编译与运行

### 编译项目

```bash
mvn clean package
```

### 运行项目

```bash
# 本地运行
java -jar target/mongodb-to-starrocks-1.0.0.jar

# Flink集群运行
flink run -c com.datasync.job.MongodbToStarrocksJob target/mongodb-to-starrocks-1.0.0.jar
```

## StarRocks 表设计建议

在 StarRocks 中创建对应的表时，建议添加 `isDeleted` 字段用于标记数据是否被删除：

```sql
CREATE TABLE machine_energy_history (
    _id STRING,
    machine_id BIGINT,
    energy DOUBLE,
    timestamp BIGINT,
    time STRING,
    only STRING,
    is_bigdata_calculate TINYINT,
    create_time STRING,
    isDeleted INT DEFAULT 0,
    PRIMARY KEY(_id)
)
DISTRIBUTED BY HASH(_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
``` 