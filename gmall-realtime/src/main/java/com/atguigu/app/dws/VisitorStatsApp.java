package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.VisitorStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * 1.数据流:web/app->nginx->日志服务器->Kafka(ODS)->FlinkApp->  Kafka(DWD) -> FlinkApp ->kafka(dwm) -> FlinkApp -> ClickHouse
 * 2.程序:MockLog-> nginx->Logger->   Kafka(zk)-> BaseLogApp-> Kafka -> uv/uj -> kafka -> VisitorStatsApp -> ClickHouse
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        // 开启ck 以及指定状态后端
//        env.enableCheckpointing(5 * 60000L);
//        // 并发检查点尝试的最大次数
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        // 出发下一个检查点之前的最小暂停
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // 重启策略
        // 现在默认三次
//        env.setRestartStrategy();

//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/"))

        // TODO 2.读取Kafka数据创建流
        String groupId = "visitor_stats_app_210726";

        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String pageViewSourceTopic = "dwd_page_log";

        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKakfaSource(uniqueVisitSourceTopic, groupId));

        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getKakfaSource(userJumpDetailSourceTopic, groupId));

        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKakfaSource(pageViewSourceTopic, groupId));

        // TODO 3.将每个流处理成相同的数据类型

        // 3.1 处理UV数据
        // 访客数统计
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUvDS = uvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            // 提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts"));
        });

        // 3.2 处理UJ数据
        // 跳出次数
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUjDS = ujDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            // 提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jsonObject.getLong("ts"));
        });

        // 3.3 处理PV数据 (页面访问)
        //
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPvDS = pvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            // 获取公共字段
            JSONObject common = jsonObject.getJSONObject("common");
            // 获取页面信息
            JSONObject page = jsonObject.getJSONObject("page");

            String last_page_id = page.getString("last_page_id");

            long sv = 0L;

            if (last_page_id == null || last_page_id.length() <= 0) {
                sv = 1L;
            }

            return new VisitorStats("", "",
                    common.getString("vc"),  // 版本
                    common.getString("ch"),  // 渠道
                    common.getString("ar"),  // 地区
                    common.getString("is_new"),  // 是否新老用户
                    0L, 1L, sv, 0L, page.getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        // TODO 4.Union几个流
        DataStream<VisitorStats> unionDS = visitorStatsWithUvDS.union(visitorStatsWithUjDS, visitorStatsWithPvDS);

        // TODO 5.提取时间戳生成WaterMark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWaterMark = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        // TODO 6.按照维度信息分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWaterMark.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<String, String, String, String>(
                        value.getAr(), // 地区
                        value.getCh(), // 渠道
                        value.getIs_new(), // 新老用户标识
                        value.getVc()); // 版本
            }
        });

        // TODO 7.开窗聚合  10s的滚动窗口
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<VisitorStats> result = windowDStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                // 滑动窗口只能采用new的方法;
//                return new VisitorStats(
//                        value1.getStt(),
//                        value1.getEdt(),
//                        value1.getVc(),
//                        value1.getCh(),
//                        value1.getAr(),
//                        value1.getIs_new(),
//                        value1.getUv_ct()+value2.getUv_ct(),
//                        );
                // 滚动窗口可以采用下面写法
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());

                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                long start = window.getStart();
                long end = window.getEnd();

                VisitorStats visitorStats = input.iterator().next();

                // 补充窗口信息
                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));

                out.collect(visitorStats);
            }
        });

        result.print("result>>>>>>");

        // TODO 8.将数据写入ClickHouse
        result.addSink(ClickHouseUtil.getClinkHouseSink("insert into visitor_stats_210726 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        // TODO 9.任务的启动
        env.execute("VisitorStatsApp");
    }
}

























