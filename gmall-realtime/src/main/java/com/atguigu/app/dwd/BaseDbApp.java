package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.DimSinkFunction;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.app.func.MyDeserialization;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启CK 以及 指定状态后端
//        env.enableCheckpointing(5 * 60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        env.setRestartStrategy();
//
//        env.setStateBackend(new FsStateBackend(""))

        // TODO 2.读取Kafka ods_base_db 主题的数据创建流
        DataStreamSource<String> kafkaDStream = env.addSource(MyKafkaUtil.getKakfaSource("ods_base_db", "base_db_app_210726"));

        // TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDStream.map(JSON::parseObject);

//        kafkaDStream.map(line -> JSON.parseObject(line));

        // TODO 4.过滤空数据（删除数据）   主流
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return !"delete".equals(value.getString("type"));
            }
        });

        // TODO 5.使用FlinkCDC读取配置表，创建广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .password("123456")
                .username("root")
                .databaseList("gmall-210726-realtime")
                .tableList("gmall-210726-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserialization())
                .build();

        DataStreamSource<String> flinkCDCDS = env.addSource(sourceFunction);

//        flinkCDCDS.print("dbapp: tb流");

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);

        BroadcastStream<String> broadcastStream = flinkCDCDS.broadcast(mapStateDescriptor);

        // TODO 6.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);

        // TODO 7.处理广播流数据和主流数据   分为Kafka和HBase流
        // 获取侧输出流的标记
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbaseTag") {
        };

        SingleOutputStreamOperator<JSONObject> kafkaMainDS = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        // TODO 8.提取两个流的数据
        DataStream<JSONObject> hbaseDS = kafkaMainDS.getSideOutput(hbaseTag);

        // TODO 9.将两个流的数据分别写出
        kafkaMainDS.print("Kafka>>>>>");
        hbaseDS.print("hbase>>>>>");

        hbaseDS.addSink(new DimSinkFunction());

        kafkaMainDS.addSink(MyKafkaUtil.getKafkaSink(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                //A key/value pair to be sent to Kafka. This consists of a topic name to which the record is being sent, an optional partition number, and an optional key and value.
                //要发送到 Kafka 的键值对。这包括记录要发送到的主题名称、可选的分区号以及可选的键和值。
                return new ProducerRecord<>(element.getString("sinkTable"), element.getString("afterJSON").getBytes());
            }
        }));


        // TODO 10.启动任务
        env.execute("BaseDbApp");

    }
}








