package com.ziyun.datasync.mongodb;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


@Slf4j
public class MongodbToStarrocksJob {
    public static void main(String[] args) throws Exception {
        System.setProperty("jdk.module.illegalAccess.silent", "true");
        log.info("启动MongoDB到StarRocks同步任务");

        // 创建Flink执行环境
        Configuration config = new Configuration();
        // 启用Web UI
        config.setBoolean(WebOptions.SUBMIT_ENABLE, true);
        // 设置Web UI端口，不设置会随机选择
        config.setInteger(RestOptions.PORT, 8084);
        // 使用配置创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        // 创建Flink执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建Table环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        // 创建MongoDB CDC源表
        createMongoDBSourceTable(tenv);

        tenv.executeSql("select * from machine_energy_history").print();

        env.execute("Mongodb能耗表同步");
    }


    /**
     * 创建MongoDB CDC源表
     *
     * @param tenv Table环境
     */
    private static void createMongoDBSourceTable(StreamTableEnvironment tenv) {
        String createTableSQL = String.format(
                "CREATE TABLE machine_energy_history (" +
                        "   _id STRING," +
                        "   `machine_id` BIGINT," +
                        "   `energy` DOUBLE," +
                        "   `timestamp` BIGINT," +
                        "   `time` STRING," +
                        "   `only` STRING," +
                        "   `is_bigdata_calculate` TINYINT," +
                        "   `create_time` STRING," +
                        "   PRIMARY KEY(_id) NOT ENFORCED" +
                        ") WITH (" +
                        "   'connector' = 'mongodb-cdc'," +
                        "   'hosts' = '%s'," +
                        "   'username' = '%s'," +
                        "   'password' = '%s'," +
                        "   'database' = '%s'," +
                        "   'collection' = '%s'," +
                        "   'scan.startup.mode' = 'latest-offset'" +
                        ")",
                "127.0.0.1:27617",
                "mongouser",
                "Pass1234",
                "ziyun-digital",
                "machine_energy_history"
        );

        log.info("创建MongoDB CDC源表");
        tenv.executeSql(createTableSQL);
    }
}
