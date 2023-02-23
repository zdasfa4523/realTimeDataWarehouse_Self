package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.bean.TransientSink;
import com.atguigu.gmall.realtime.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyClickhouseUtil {
    public static <T> SinkFunction<T> getSinkFunction(String sql) {
        /*return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        // 占位符赋值 preparestatement  要将t里面的属性取出来给ck sql中的占位符赋值

                        Class<?> clazz = t.getClass();
                        Field[] fields = clazz.getDeclaredFields();
                        int offset = 0;
                        for (int i = 0; i < fields.length; i++) {

                            Field field = fields[i];
                            fields[i].setAccessible(true);

                            // 尝试获取字段上的注解信息
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                offset++;
                                // 说明字段上有这个注解
                                // 这个字段不需要了,跳过当前字段
                                continue;
                            }

                            // 注意反射调用属性的时候是 fields[i].get(clazz) 反射调用
                            preparedStatement.setObject(i + 1 -offset, fields[i].get(clazz));
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());*/


        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        //通过反射的方式获取属性值
                        Class<?> clz = t.getClass();

//                        Method[] methods = clz.getMethods();
//                        for (Method method : methods) {
//                            method.invoke(t);
//                        }

                        //Field[] fields = clz.getFields();
                        Field[] fields = clz.getDeclaredFields();

                        //遍历字段,获取每个字段对应的值并将其写入预编译SQL中
                        int offset = 0;
                        for (int i = 0; i < fields.length; i++) {

                            Field field = fields[i];
                            field.setAccessible(true);

                            //尝试获取字段上的注解
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                offset++;
                                continue;
                            }

                            Object value = field.get(t);
                            preparedStatement.setObject(i + 1 - offset, value);
                        }
                    }
                },
                new JdbcExecutionOptions
                        .Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }
}
