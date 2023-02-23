package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.DimHandler;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import com.atguigu.gmall.realtime.util.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;


// 为了给其他的类使用 我们不使用具体的bean类
public abstract class AsyncDimFunction <T> extends RichAsyncFunction<T, T> implements AsyncJoinFunction<T> {

    // 初始化线程池
    private ThreadPoolExecutor threadPoolExecutor;
    // 初始化DRUID 连接池
    private DruidPooledConnection connection;

    // 表信息属性
    private String tableName;
    public AsyncDimFunction(String tableName) {
        this.tableName = tableName;
    }



    @Override
    public void open(Configuration parameters) throws Exception {
        // 给线程池赋值
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
        // 给连接池赋值
        connection = DruidDSUtil.getDruidDataSource().getConnection();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.execute(new Runnable() {


            @SneakyThrows
            @Override
            public void run() {

                // 查询维表
                // 遇到的问题 : 如何得到查询的表信息?
                // 通过构造器 实现外部传入
                
                

                // 遇到的问题: 主键从哪里来的?
                // 1. 利用反射得到对应的列信息,列名可以通过外部传参传进来进行比较
                // 2. 向上抽象
                String key = getKey(input);
                JSONObject dimInfo = DimHandler.getDimInfo(connection, tableName, key);

                // 补充信息
                // 防止查询到的信息为null
                if (dimInfo != null) {
                // 要往这个bean中补充查询出来的字段信息 但是我们发现我们发现每一张表中要补充的的字段以及数量都不一致 因此也是将这个方法向上抽象
                    // 构建join方法 将需要补充的javabean以及维度信息补充到形参中
                    join(input,dimInfo);
                }


                // 写出数据 --> resultFuture来做
                resultFuture.complete(Collections.singletonList(input));

                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }




        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut QAQ...");
    }
}

