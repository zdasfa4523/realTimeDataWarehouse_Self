package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import com.atguigu.gmall.realtime.util.JedisUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.sql.PreparedStatement;
import java.util.*;

public class DIM_SinkFunction extends RichSinkFunction<JSONObject> {
    //只有RichSinkFunction才会有open方法 用来构建连接池,减少资源
    private DruidDataSource druidDataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 构建链接池对象
        druidDataSource = DruidDSUtil.getDruidDataSource();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        // 获取链接
        DruidPooledConnection connection = druidDataSource.getConnection();
        System.out.println(connection);

        // 拼接sql语句
        // upsert into db.tn(id,name,sex) values('1001','zhangsan','male')
        StringBuilder sql = genUpsertSql(value.getString("sink_table"), value.getJSONObject("data"));

        JSONObject data = value.getJSONObject("data");

        // todo 判斷如果為更新數據 則先將數據寫入到Redis
        if ("update".equals(value.getString("type"))) {
            Jedis jedis = JedisUtil.getJedis();

            // 寫入到Redis中
            String redisKey = "DIM:" + value.getString("sink_table").toUpperCase() +":" + data.getString("id");

            JSONObject jsonObject = new JSONObject();

            Set<Map.Entry<String, Object>> entries = data.entrySet();
            Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                jsonObject.put(entry.getKey().toUpperCase(),entry.getValue().toString());
            }

            jedis.set(redisKey,jsonObject.toJSONString());

            jedis.expire(redisKey,24*3600);

            jedis.close();


        }

        // 将数据写出
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());

        preparedStatement.execute();
        connection.commit();

        // 释放资源
        preparedStatement.close();
        connection.close();

    }

    private StringBuilder genUpsertSql(String sinkTable, JSONObject data) {
        // 拼接sql语句
        // upsert into db.tn(id,name,sex) values('1001','zhangsan','male')
        // 取出列名和列值
        // 问题:为啥是keyset?
        // 回答: 取出map中的key方法 values()拿到map中的value数据
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();
        // 使用StringBuilder拼接字符串
        // 如果有一亿条数据的话 堆里面会生成一亿个StringBuilder对象吗?
        StringBuilder sql = new StringBuilder();
        StringBuilder upsertsql = sql.append("upsert into ").append(GmallConfig.PHOENIX_DB).append(".").append(sinkTable).append("(").append(StringUtils.join(columns,",")).append(") values ('").append(StringUtils.join(values,"','")).append("')");

        System.out.println(upsertsql);

        return upsertsql;
    }
}
