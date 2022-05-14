package com.huc.app;

import com.huc.been.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WaterMark01 {
    public static void main(String[] args) throws Exception {
        // 1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        // 提取时间戳生成watermark
        SingleOutputStreamOperator<String> watermarks = dataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] fields = element.split(",");
                        return Long.parseLong(fields[1]) * 1000L;
                    }
                }));

        // 转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSersorDS = watermarks.map(line -> {
            String[] fields = line.split(",");
            return new WaterSensor(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });


        waterSersorDS.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        return new WaterSensor(value1.getId(), value1.getTs(), Math.max(value1.getVc(), value2.getVc()));
                    }
                }).print();

        env.execute();

    }
}























