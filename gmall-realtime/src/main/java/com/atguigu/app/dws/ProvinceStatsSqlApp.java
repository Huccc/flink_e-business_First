package com.atguigu.app.dws;

import com.atguigu.bean.ProvinceStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        // TODO 2.使用DDL创建表  提取时间戳生成WaterMark
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        tableEnv.executeSql("CREATE TABLE ORDER_WIDE (" +
                "province_id BIGINT, " +
                "province_name STRING,province_area_code STRING," +
                "province_iso_code STRING,province_3166_2_code STRING,order_id STRING, " +
                "total_amount DOUBLE,create_time STRING,rowtime AS TO_TIMESTAMP(create_time) ," +
                "WATERMARK FOR rowtime AS rowtime)" +
                " WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")");

        // TODO 3.查询数据  分组、开窗、聚合
        Table table = tableEnv.sqlQuery("select " +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                " province_id,province_name,province_area_code," +
                "province_iso_code,province_3166_2_code," +
                "COUNT( DISTINCT order_id) order_count, sum(total_amount) order_amount," +
                "UNIX_TIMESTAMP()*1000 ts " +
                " from " +
                "ORDER_WIDE group by TUMBLE(rowtime, INTERVAL '10' SECOND )," +
                " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ");

        // TODO 4.将动态表转换为流
        DataStream<ProvinceStats> provinceStatsDataStream =
                tableEnv.toAppendStream(table, ProvinceStats.class);

        // TODO 5.打印数据并写入ClickHouse
        provinceStatsDataStream.print();

        provinceStatsDataStream.addSink(ClickHouseUtil.
                <ProvinceStats>getClinkHouseSink("insert into province_stats_210726 values(?,?,?,?,?,?,?,?,?,?)"));

        // TODO 6.启动任务
        env.execute("ProvinceStatsSqlApp");
    }
}


















