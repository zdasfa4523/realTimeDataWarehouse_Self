package com.atguigu.gmall.realtime.util;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

public class DimHandler {




    // DWS 層爲表關聯優化 使用内存旁路緩存 (讀寫分離)

    // 未來寫數據找 HBASE
    // 讀取數據 找redis 在查HBASE 之前先查REDIS 如果Redis中查不到 Redis無法命中 再去查HBASE
    // Hbase中可以查到 之後寫入redis内
    // 相當於 HBASE 只跟Redis做對接hbase將查到的數據存入到Redis中



    public static JSONObject getDimInfo(Connection connection, String tableName, String key) throws Exception {
        // 查詢redis中的數據
        Jedis jedis = JedisUtil.getJedis();

        // 拼接查詢的key
        // key如何設計? 如同之前寫的Hbase寫的查詢語句 只需要確認 查詢的表以及主鍵即可
        String redisKey = "DIM:"+tableName+":"+key;
        String jsonStr = jedis.get(redisKey); // 獲取到的String類型 但是需要的返回值的類型是JSONObject
        // 第一次訪問必然Redis中沒有數據 因此必然為null

        if (jsonStr != null) {
            // 黨有查詢的時候 重置過期時間
            jedis.expire(redisKey,24*3600);
            // 歸還所需要的鏈接
            jedis.close();
            return JSONObject.parseObject(jsonStr);
        }




        // 查詢HBASE 中的數據
        String sql = "select * from "+ GmallConfig.PHOENIX_DB+"."+tableName + " where id =  '" + key +"'";
        System.out.println("sql為 :"+sql);

        List<JSONObject> queryList = JDBCUtil.queryList(connection, sql, JSONObject.class, false);

        // 雖然叫做list 但是由於是主鍵查詢 因此結果只會有一條

        // 將HBASE查詢出來的數據寫入到Jedis
        JSONObject jsonObject = queryList.get(0);
        // 寫入到redis中 並設置過期時間
        jedis.setex(redisKey,24*3600,jsonObject.toJSONString());
        jedis.close();

        // 返回結果
        return queryList.get(0);

    }

    public static void main(String[] args) throws Exception {
        DruidPooledConnection connection = DruidDSUtil.getDruidDataSource().getConnection();
        JSONObject  gmall_realtime_220828 = getDimInfo(connection, "DIM_BASE_TRADEMARK", "30");

        System.out.println(gmall_realtime_220828);

        connection.close();
    }

}
