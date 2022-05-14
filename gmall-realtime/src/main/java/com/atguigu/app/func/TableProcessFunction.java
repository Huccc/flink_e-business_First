package com.atguigu.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.*;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    // 声明Phoenix连接
    private Connection connection;

    // 侧输出流标记属性
    private OutputTag<JSONObject> outputTag;

    // Map状态描述器属性
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;


    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 加载驱动
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    // value:{"database":"gmall-210726-realtime","beforeJSON":{},"type":"insert","tableName":"table_process","afterJSON":{"operate_type":"insert","sink_type":"hbase","sink_table":"dim_base_trademark","source_table":"base_trademark","sink_pk":"id","sink_columns":"id,tm_name"}}
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        // 1.解析数据为TableProcess对象
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("afterJSON"), TableProcess.class);

        System.out.println("监控到的tb： " + tableProcess);

        // 2.建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(
                    tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
            );
        }

        // 3.写入状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }

    // 创建Phoenix表：create table if not exists db.tb(id varchar primary key,name varchar,sex varchar) xxxx;
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        if (sinkPk == null) {
            sinkPk = "id";
        }

        if (sinkExtend == null) {
            sinkExtend = "";
        }

        PreparedStatement preparedStatement = null;

        try {
            // 构建建表语句
            StringBuilder createTableSQL = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                // 取出列名
                String column = columns[i];
                // 判断当前字段是否为主键
                if (sinkPk.equals(column)) {
                    createTableSQL.append(column)
                            .append(" varchar primary key ");
                } else {
                    createTableSQL.append(column)
                            .append(" varchar ");
                }

                // 判断是否为最后一个字段
                if (i < columns.length - 1) {
                    createTableSQL.append(",");
                }
            }
            createTableSQL.append(")")
                    .append(sinkExtend);

            // 打印建表语句
            System.out.println(createTableSQL);

            // 执行
            preparedStatement = connection.prepareStatement(createTableSQL.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("创建Phoenix表->" + sinkTable + "失败!");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }

    }

    // value:{"database":"gmall-210726-realtime","tableName":"table_process","afterJSON":{"operate_type":"insert","sink_type":"hbase","sink_table":"dim_base_trademark","source_table":"base_trademark","sink_pk":"id","sink_columns":"id,tm_name"}}
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 1.获取广播状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "-" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            // 2.过滤字段
            filterColumns(value.getJSONObject("afterJSON"), tableProcess.getSinkColumns());

            // 3.分流  Kafka主流  Hbase侧输出流

            // 将表名或者主题名添加至数据中
            value.put("sinkTable", tableProcess.getSinkTable());

            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                ctx.output(outputTag, value);
            }

        } else {
            System.out.println(key + "不存在！");
        }

    }

    private void filterColumns(JSONObject data, String sinkColumns) {
        // 切分字段
        String[] columns = sinkColumns.split(",");
        List<String> columnList = (List<String>) Arrays.asList(columns);

        // 遍历data
//        Set<Map.Entry<String, Object>> entries = data.entrySet();
//        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columnList.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !columnList.contains(next.getKey()));

    }


}















