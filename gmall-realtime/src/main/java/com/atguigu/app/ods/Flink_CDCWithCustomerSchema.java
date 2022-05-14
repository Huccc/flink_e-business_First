package com.atguigu.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.MyDeserialization;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink_CDCWithCustomerSchema {
    public static void main(String[] args) throws Exception {
        // 1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO Flink-CDC将读取binlog的位置信息以状态的方式保存在CK，如果想要做到断点续传，需要从Checkpoint或者Savepoint启动程序
        // todo 1.开启checkpoint，每隔5秒钟做一次CK
        env.enableCheckpointing(5000L);
        // todo 2 指定CK的一致性语义        默认就是->EXACTLY_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // todo 3 设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // todo 4 指定从CK自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        // todo 5 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));
        // todo 6 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // 2.设置Mysql的相关属性
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_flink_0726")
//                .tableList("gmall_flink_0726.base_sale_attr")
//                .startupOptions(StartupOptions.initial())
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyDeserialization())
                .build();

        // 3.将flinkCDC添加到source中
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        // 将数据发送至Kafka
        streamSource.print();
        streamSource.addSink(MyKafkaUtil.getKafkaSink("ods_base_db"));

        env.execute();

    }


}
















