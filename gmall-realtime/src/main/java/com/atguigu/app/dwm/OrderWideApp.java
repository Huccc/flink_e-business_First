package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.AsyncDimFunction;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

//

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
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

        // TODO 2.读取Kafka 订单和订单明细数据创建流
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group_210726";
        DataStreamSource<String> orderInfoStrDS = env.addSource(MyKafkaUtil.getKakfaSource(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderDetailStrDS = env.addSource(MyKafkaUtil.getKakfaSource(orderDetailSourceTopic, groupId));


        // TODO 3.将每行数据转换为JavaBean对象并提取时间戳生成WaterMark
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoStrDS.map(line -> {
            OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);
            // 处理数据中的时间
            String create_time = orderInfo.getCreate_time();
            String[] dataTimeArr = create_time.split(" ");

            orderInfo.setCreate_date(dataTimeArr[0]);
            orderInfo.setCreate_hour(dataTimeArr[1].split(":")[0]);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            orderInfo.setCreate_ts(sdf.parse(create_time).getTime());
            return orderInfo;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts() * 1000L;
                    }
                }));

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailStrDS.map(line -> {
            OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
            // 补充时间戳字段
            String create_time = orderDetail.getCreate_time();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderDetail.setCreate_ts(sdf.parse(create_time).getTime());

            return orderDetail;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts() * 1000L;
                    }
                }));

        // TODO 4.双流join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                // 给定公司中最大的延迟时间
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        orderWideDS.print("orderWideDS>>>>>>>");

        // TODO 5.关联维表
//        orderWideDS.map(orderWide->{
//
//            // 关联用户维度，查询用户维度
//
//            // 查询省份维度
//            return orderWide;
//        });

        // 5.1 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideDS,
                new AsyncDimFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setUser_gender(dimInfo.getString("GENDER"));

                        String birthday = dimInfo.getString("BIRTHDAY");
                        long ts = System.currentTimeMillis();

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        long birthTs = sdf.parse(birthday).getTime();

                        long age = (ts - birthTs) / (1000L * 60 * 60 * 24 * 365);

                        orderWide.setUser_age((int) age);
                    }
                },
                60,
                TimeUnit.SECONDS
        );

//        orderWideWithUserDS.print("user>>>>>>>>>");

        // 5.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new AsyncDimFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setProvince_name(dimInfo.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }
                }, 60, TimeUnit.SECONDS);


        // 5.2 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS,
                new AsyncDimFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }
                }, 60, TimeUnit.SECONDS);

        // 5.2 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(orderWideWithSkuDS, new AsyncDimFunction<OrderWide>("DIM_SPU_INFO") {
            @Override
            public String getKey(OrderWide orderWide) {
                return orderWide.getSpu_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
            }
        }, 60, TimeUnit.SECONDS);

        // 5.2 关联TM维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new AsyncDimFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        // 5.2 关联Category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new AsyncDimFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print("orderWideWithCategory3DS");

        // TODO 6.将数据写出到Kafka的主题
        orderWideWithCategory3DS
                .map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

        // TODO 7.启动任务
        env.execute("OrderWideApp");

    }
}
















