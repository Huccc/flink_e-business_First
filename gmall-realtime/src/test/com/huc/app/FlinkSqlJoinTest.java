package com.huc.app;

import com.huc.been.Bean1;
import com.huc.been.Bean2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class FlinkSqlJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设置状态保存的一个过期时间
        // PT processing time 处理时间  设置为0 相当于状态清楚的参数被关了
        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        System.out.println(tableEnv.getConfig().getIdleStateRetention());

        SingleOutputStreamOperator<Bean1> b1DS = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Bean1(split[0], split[1], Long.parseLong(split[2]));
                });

        SingleOutputStreamOperator<Bean2> b2DS = env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, Bean2>() {
                    @Override
                    public Bean2 map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Bean2(split[0], split[1], Long.parseLong(split[2]));
                    }
                });

        // 将流转换为动态表
        tableEnv.createTemporaryView("t1", b1DS);
        tableEnv.createTemporaryView("t2", b2DS);

        // 双流join
        // executeSql 主要是测试的时候使用
        // 内连接  左：OnCreateAndWrite  右：OnCreateAndWrite
//        tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 join t2 on t1.id=t2.id")
//                .print();

        // 左外连接  左：OnReadAndWrite  右：OnCreateAndWrite
//        tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 left join t2 on t1.id=t2.id").print();

        // 右外连接  左：OnCreateAndWrite  右：OnReadAndWrite
//        tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 right join t2 on t1.id=t2.id").print();

        // 全外连接  左：OnReadAndWrite  右：OnReadAndWrite
        tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 full join t2 on t1.id=t2.id").print();

//        env.execute();

    }
}














