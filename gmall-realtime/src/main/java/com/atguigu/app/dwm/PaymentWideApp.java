package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentInfo;
import com.atguigu.bean.PaymentWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

// 数据流：web/app -> nginx -> SpringBoot -> Mysqsl -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka/phoenix(dwd-dim) -> FlinkApp(redis) -> Kafka(dwn) -> FlinkApp -> Kafka(dwm)
// 程序：MockDb -> Mysql -> FlinkCDC -> Kafka(zk) -> BaseDbApp -> Kafka/Phoenix(zk/hdfs/hbase) -> OrderWideApp(Redis) -> Kafka -> PaymentWideApp -> Kafka
public class PaymentWideApp {
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

        // TODO 2.读取Kafka主题的数据创建流 并转换为JavaBean对象，同时提取时间戳生成WaterMark
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        // 订单表
        KeyedStream<OrderWide, Long> orderWideKeyedStream = env.addSource(MyKafkaUtil.getKakfaSource(orderWideSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return recordTimestamp;
                                }
                            }
                        }))
                .keyBy(OrderWide::getOrder_id);

        // 支付表
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedStream = env.addSource(MyKafkaUtil.getKakfaSource(paymentInfoSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return recordTimestamp;
                                }
                            }
                        }))
                .keyBy(PaymentInfo::getOrder_id);

        // TODO 3.双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedStream.intervalJoin(orderWideKeyedStream)
                .between(Time.minutes(-15), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });

        paymentWideDS.print("paymentWideDS>>>>>");

        // TODO 4.将数据写入Kafka
        paymentWideDS.map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        // TODO 5.启动任务
        env.execute();

    }
}
