package com.atguigu.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;


public class FlinkCDC_DataStream_API_Deseriali {
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
                .tableList("gmall_flink_0726.z_user_info")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeseriali())
                .build();

        // 3.将flinkCDC添加到source中
        env.addSource(sourceFunction)
                .print();


        env.execute();

    }

    public static class MyDeseriali implements DebeziumDeserializationSchema<String> {

        /**
         * 最外层是个json，里面数据是一个json
         * {
         * "database":"",
         * "tableName":"",
         * "after":{"id":"1001","name":"zhangsan"....},
         * "before":{"id":"1001","name":"zhangsan"....},
         * "type":"insert"
         * }
         *
         * @param sourceRecord
         * @param collector
         * @throws Exception
         */

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            // 1.创建JSONObject对象来存放最终结果
            JSONObject result = new JSONObject();

            // TODO 获取数据库&表名
            String topic = sourceRecord.topic();
            String[] split = topic.split("\\.");
            String database = split[1];
            String tableName = split[2];

            // TODO 获取before&after数据
            Struct value = (Struct) sourceRecord.value();

            // TODO after
            Struct after = value.getStruct("after");
            JSONObject afterJSON = new JSONObject();
            // 判断是否有after数据
            if (after != null) {
                Schema schema = after.schema();
                List<Field> fieldList = schema.fields();
                for (Field field : fieldList) {
                    afterJSON.put(field.name(), after.get(field));
                }
            }

            // TODO before
            Struct before = value.getStruct("before");
            JSONObject beforeJSON = new JSONObject();
            // 判断是否有before数据
            if (before != null) {
                Schema schema = before.schema();
                List<Field> fieldList = schema.fields();
                for (Field field : fieldList) {
                    beforeJSON.put(field.name(), before.get(field));
                }
            }

            // 获取操作类型 DELETE UPDATE CREATE
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            String type = operation.toString().toLowerCase();
            // 自定义改类型
            if ("create".equals(type)) {
                type = "insert";
            }

            result.put("database", database);
            result.put("tableName", tableName);
            result.put("afterJSON", afterJSON);
            result.put("beforeJSON", beforeJSON);
            result.put("type", type);

            collector.collect(result.toJSONString());

        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}






