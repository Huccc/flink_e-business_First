package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    // 声明Phoenix连接
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 加载驱动
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        // mysql中默认是true phoenix中默认是false
        // 自动提交
//        connection.setAutoCommit(true);
    }

    // value:{"database":"gmall-210726-realtime","beforeJSON":{},"type":"insert","tableName":"table_process","afterJSON":{"operate_type":"insert","sink_type":"hbase","sink_table":"dim_base_trademark","source_table":"base_trademark","sink_pk":"id","sink_columns":"id,tm_name"}}

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        PreparedStatement preparedStatement = null;
        try {
            // 1.拼接插入数据的sql语句  upsert into db.tn(id,name,sex) values('1001','zhangsan','male')
            String upsertSql = getUpsertSQL(value.getString("sinkTable"), value.getJSONObject("afterJSON"));

            // 2.预编译sql
            preparedStatement = connection.prepareStatement(upsertSql);

            // TODO 如果维度数据更新，则先删除Redis中的数据
            if ("update".equals(value.getString("type"))) {
                DimUtil.delDimInfo(value.getString("sinkTable").toUpperCase(), value.getJSONObject("afterJSON").getString("id"));
            }

            // 3.执行写入数据操作
            preparedStatement.execute();

            // 数据量大的时候批处理
//        preparedStatement.addBatch();

            // 提交
            connection.commit();
        } catch (SQLException e) {
            System.out.println("插入数据失败!");
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }

    // upsert into db.tn(id,name,sex) values('1001','zhangsan','male')
    private String getUpsertSQL(String sinkTable, JSONObject afterJSON) {

        // 获取列名和数据
        Set<String> columns = afterJSON.keySet();
        Collection<Object> values = afterJSON.values();

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(columns, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";
    }
}





















