package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

// 数据流：web/app -> Nginx -> 日志服务器 ->kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> kafka(DWM)
// 程序：mock->      nginx -> logger.sh  ->kafka(zk) -> baselogapp->kafka(zk) -> userjumpdetailapp->kafka(zk)
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        // todo 设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME", "atguigu");
//
//
//        // 开启ck 以及指定状态后端
//        //@param interval Time interval between state checkpoints in milliseconds.
//        //状态检查点之间的时间间隔（以毫秒为单位）。
//        env.enableCheckpointing(5 * 60000L);    //5min
//        //@param maxConcurrentCheckpoints The maximum number of concurrent checkpoint attempts.
//        //并发检查点尝试的最大次数。(允许最多同时存在的个数)
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        //@param minPauseBetweenCheckpoints The minimal pause before the next checkpoint is triggered.
//        //触发下一个检查点之前的最小暂停。
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//
//        // 重启策略 现在默认三次
////        env.setRestartStrategy();
//
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/test"));

        // TODO 2.读取页面主题的数据创建流
        String sourceTopic = "dwd_page_log";
        String groupId = "user_Jump_detail_app_210726";
        String sinkTopic = "dwm_user_jump_detail";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKakfaSource(sourceTopic, groupId));

        // TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        // TODO 4.提取事件时间生成WaterMark
        SingleOutputStreamOperator<JSONObject> jsonObjectSingleOutputStreamOperator = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        // TODO 5.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjectSingleOutputStreamOperator.keyBy(json -> json.getJSONObject("common").getString("mid"));

        // TODO 6.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                }).next("next")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                }).within(Time.seconds(10));

        // TODO 7.将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // TODO 8.提取事件(匹配上的事件和超时事件)
        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("timeOut") {
        };
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(outputTag,
                // 超时的
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("start").get(0);
                    }
                },
                // 用于去匹配上的事件
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0);
                    }
                });

        // TODO 9.结合两个流
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(outputTag);
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);

        // TODO 10.将数据写入Kafka
        unionDS.print();
        unionDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        // TODO 11.启动任务
        env.execute("UserJumpDetailApp");
    }
}


















