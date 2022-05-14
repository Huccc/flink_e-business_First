package com.atguigu.utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {
    public static <T> SinkFunction<T> getClinkHouseSink(String sql) {


        return JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        try {
                            // 通过反射的方式获取当前JavaBean的所有字段
                            Class<?> clz = t.getClass();
//                        clz.getFields();  只能获取公共字段
                            Field[] fields = clz.getDeclaredFields();

                            // 获取方法名称
//                            Method[] methods = clz.getMethods();
//                            for (int i = 0; i < methods.length; i++) {
//                                Method method = methods[i];
//                                Object invoke = method.invoke(t);
//                            }

                            // 遍历字段数组
                            int j = 0;
                            for (int i = 0; i < fields.length; i++) {
                                // 获取字段名称
                                Field field = fields[i];

                                // 获取字段上的注解
                                TransientSink transientSink = field.getAnnotation(TransientSink.class);
                                if (transientSink != null) {
                                    j++;
                                    continue;
                                }

                                // 设置该字段的访问权限
                                field.setAccessible(true);

                                // 获取该字段对应的值信息
                                Object value = null;

                                value = field.get(t);

                                // 给预编译SQL占位符赋值
                                preparedStatement.setObject(i + 1 - j, value);
                            }
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }

                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
    }
}



















