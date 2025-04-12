package com.ziyun.datasync.mongodb;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;

import org.apache.flink.configuration.WebOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;


@Slf4j
public class MongodbToStarrocksStreamAPIJob {
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


        // 高级配置示例
        MongoDBSource<String> mongoDBSource = MongoDBSource.<String>builder()
                .hosts("127.0.0.1:27617")
                .databaseList("ziyun-digital")
                .collectionList("ziyun-digital.machine_energy_history")
                .username("ziyunbd")
                .password("m3A9Kzfyep")
                .startupOptions(StartupOptions.latest())
                .pollAwaitTimeMillis(1000)
                .pollMaxBatchSize(1024)  // 每批最大拉取数量
                .deserializer(new JsonDebeziumDeserializationSchema())

                .build();

        // 创建MongoDB CDC Source
        /*DebeziumSourceFunction<MachineEnergyHistory> mongoDBSource = createMongoDBSource();
        // 添加Source到环境中
        DataStream<MachineEnergyHistory> mongoStream = env.addSource(mongoDBSource)
                .name("MongoDB-CDC-Source");
        // 为source添加日志记录，帮助排查问题
        mongoStream.map(data -> {
            log.info("收到MongoDB数据: {}", data);
            return data;
        }).name("Data-Logger");*/
        DataStream<String> mongoStream = env.fromSource(
                mongoDBSource,
                WatermarkStrategy.noWatermarks(),  // 因为CDC事件自带时间戳或不需要Watermark
                "MongoDBCDCSource"
        ).setParallelism(1);

        // 打印原始流，用于调试
        mongoStream.process(new ProcessFunction<String, Void>() {
            @Override
            public void processElement(String row, Context ctx, Collector<Void> out) {
                log.info("收到MongoDB原始数据: {} ", row);
            }
        }).name("Debug-Logger");

//        tenv.executeSql("select * from machine_energy_history").print();

        env.execute("Mongodb能耗表同步");
    }

}
