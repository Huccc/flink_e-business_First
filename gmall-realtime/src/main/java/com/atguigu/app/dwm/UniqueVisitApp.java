package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

// 数据流: web/app -> Nginx -> 日志服务器 -> kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> Kafka(DWM)
// 程序: Mock     -> Nginx  -> logger.sh -> kafka(zk) -> BaseLogApp -> kafka(zk) -> uniquevisitapp->kafka(zk)
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
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


        // TODO 2.消费Kafka dwd_page_log 主题的数据
        String groupId = "unique_visit_app_210726";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKakfaSource(sourceTopic, groupId));

        // TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        // TODO 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // TODO 5.使用状态编程对非今天访问的第一条数据做过滤
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> dtState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("dt-state", String.class);

                // 设置ttl
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);

                dtState = getRuntimeContext().getState(valueStateDescriptor);

                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                // 获取上一条页面ID
                String lastPageId = value.getJSONObject("page").getString("last_page_id");

                if (lastPageId == null || lastPageId.equals("")) {
                    // 取出状态数据
                    String dt = dtState.value();

                    // 获取今天的日期
                    Long ts = value.getLong("ts");
                    String currentDT = sdf.format(ts);
                    // 或者当前日期大于状态日期，才更新
                    if (dt == null || !dt.equals(currentDT)) {
                        // 更新状态并数据保留
                        dtState.update(currentDT);
                        return true;
                    }
                }

                return false;
            }
        });

        // TODO 6.将数据写出到Kafka
        filterDS.print();
        filterDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        // TODO 7.启动任务
        env.execute("UniqueVisitApp");
    }
}













