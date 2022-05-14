package com.atguigu.app.dws;

import com.atguigu.app.func.SplitFunction;
import com.atguigu.bean.KeywordStats;
import com.atguigu.common.GmallConstant;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        // TODO 2.使用DDL方式读取Kafka数据创建表
        // TODO 2.1注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", SplitFunction.class);

        String groupId = "keyword_stats_app";
        String pageViewSourceTopic = "dwd_page_log";

        tableEnv.executeSql("CREATE TABLE page_view " +
                "(common MAP<STRING,STRING>, " +
                "page MAP<STRING,STRING>,ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                "WITH (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")");


        // TODO 3.过滤数据  上一跳页面为"search" and 搜索词 is not null
        Table fullwordView = tableEnv.sqlQuery("select page['item'] fullword ," +
                "rowtime from page_view  " +
                "where page['page_type']='keyword' " +
                "and page['item'] IS NOT NULL ");

        // TODO 4.注册UDTF，进行分词处理
        Table keywordView = tableEnv.sqlQuery("select keyword,rowtime from " + fullwordView + " ," +
                " LATERAL TABLE(ik_analyze(fullword)) as T(keyword)");

        // TODO 5.分组，开窗，聚合
        Table keywordStatsSearch = tableEnv.sqlQuery("select keyword,count(*) ct, '"
                + GmallConstant.KEYWORD_SEARCH + "' source ," +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "UNIX_TIMESTAMP()*1000 ts from   " + keywordView
                + " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),keyword");

        // TODO 6.将动态表转换为流
        DataStream<KeywordStats> keywordStatsSearchDataStream =
                tableEnv.<KeywordStats>toAppendStream(keywordStatsSearch, KeywordStats.class);

        keywordStatsSearchDataStream.print();
        // TODO 7.将数据打印并写入ClinkHouse
        keywordStatsSearchDataStream.addSink(
                ClickHouseUtil.<KeywordStats>getClinkHouseSink(
                        // JavaBean的顺序，表的列名
                        "insert into keyword_stats_210726(keyword,ct,source,stt,edt,ts)  " +
                                " values(?,?,?,?,?,?)"));

        // TODO 8.
        env.execute("KeywordStatsApp");
    }
}
