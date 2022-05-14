package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 1.数据流:web/app->nginx->日志服务器->Kafka(ODS)->FlinkApp->  Kafka(DWD)
 * 2.程序:MockLog-> nginx->Logger->   Kafka(zk)-> BaseLogApp-> Kafka
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 生产环境中应该与Kafka分区数保持一致，这样效率最大化

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


        //TODO 2.读取 Kafka dos_base_log 主题的数据创建流
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKakfaSource("ods_base_log", "base_log_app_210726"));

        //TODO 3.将数据转换为JSON对象(注意提取脏数据)
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjectDStream = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    // 将转换成功的数据写入主流
                    out.collect(jsonObject);
                } catch (Exception e) {
                    // 转换失败，将数据写入侧输出流
                    ctx.output(dirtyTag, value);
                }
            }
        });

        // 打印测试
//        jsonObjectDStream.print("JSON>>>>>>>>");
        jsonObjectDStream.getSideOutput(dirtyTag)
                .print("Dirty>>>>>>>>");

        //TODO 4.按照 mid 分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDStream.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

//        keyedStream.print("按key聚合>>>>>>>");

        //TODO 5.新老用户校验
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                // 获取新老用户标记
                String isNew = value.getJSONObject("common").getString("is_new");

                // 判断是否为“1”
                if ("1".equals(isNew)) {
                    // 获取状态数据，并判断状态是否为null
                    String state = valueState.value();
                    if (state == null) {
                        // 数据不更新，数据更新
                        valueState.update("1");
                    } else {
                        // 更新数据
                        value.getJSONObject("common").put("is_new", "0");
                    }
                }
                return value;
            }
        });

//        jsonObjWithNewFlagDS.print("JSON>>>>>>>");

        //TODO 6.分流 将页面日志->主流  启动和曝光->侧输出流
        OutputTag<JSONObject> startTag = new OutputTag<JSONObject>("start") {
        };
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {
        };
        SingleOutputStreamOperator<JSONObject> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                // 获取启动数据
                String start = value.getString("start");
                if (start != null) {
                    // 将数据写入启动日志侧输出流
                    ctx.output(startTag, value);
                } else {
                    // 将数据写入页面日志主流
                    out.collect(value);
                    // 获取曝光数据字段
                    JSONArray displays = value.getJSONArray("displays");
                    Long ts = value.getLong("ts");
                    String pageId = value.getJSONObject("page").getString("page_id");
                    if (displays != null && displays.size() > 0) {
                        // 遍历曝光数据，写出数据到曝光侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            // 补充页面id字段和时间戳字段
                            display.put("ts", ts);
                            display.put("page_id", pageId);

                            // 将数据到曝光侧输出流
                            ctx.output(displayTag, display);
                        }
                    }
                }
            }
        });

        //TODO 7.提取各个流的数据
        DataStream<JSONObject> startDS = pageDS.getSideOutput(startTag);
        DataStream<JSONObject> displayDS = pageDS.getSideOutput(displayTag);

        pageDS.print("page>>>>>>>>");
        startDS.print("start>>>>>>");
        displayDS.print("display>>>>>>>");

        //TODO 8.将数据写入 Kafka 主题
        pageDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        //TODO 9.启动任务
        env.execute();
    }
}








