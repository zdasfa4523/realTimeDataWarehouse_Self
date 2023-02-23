package com.atguigu.gmall.realtime.app.func;


import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import com.atguigu.gmall.realtime.util.JDBCUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class DIM_MyBroadcastFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

/*
    {
        "before": null,
        "after": {
                 "source_table": "financial_sku_cost",
                 "source_type": "ALL",
                 "sink_table": "dim_financial_sku_cost",
                 "sink_type": "dim",
                 "sink_columns": "id,sku_id,sku_name,busi_date,is_lastest,sku_cost,create_time",
                 "sink_pk": "id",
                 "sink_extend": null
    },
        "source": {
        "version": "1.5.4.Final",
                "connector": "mysql",
                "name": "mysql_binlog_source",
                "ts_ms": 1675951266339,
                "snapshot": "false",
                "db": "gmall-config-0828",
                "sequence": null,
                "table": "table_process",
                "server_id": 0,
                "gtid": null,
                "file": "",
                "pos": 0,
                "row": 0,
                "thread": null,
                "query": null
    },
        "op": "r",
            "ts_ms": 1675951266339,
            "transaction": null
    }
    */

    private MapStateDescriptor<String, TableProcess> stateDesc;
    public DIM_MyBroadcastFunction(MapStateDescriptor<String, TableProcess> stateDesc) {
        this.stateDesc = stateDesc;
    }

    private static DruidDataSource druidDataSource;
    // 为了避免主表中的数据先到,但是配置表中没有数据造成数据丢失的情况,
    // 可以先将主流的数据进行缓存
    // 泛型与我们在状态中保存的泛型一致
    private HashMap<String,TableProcess> tableProcessHashMap;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 创建链接池
        druidDataSource = DruidDSUtil.getDruidDataSource();
        tableProcessHashMap =  new HashMap<>();
        Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall-config-0828?" +
                "user=root&password=123456&useUnicode=true&" +
                "characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false");
        List<TableProcess> tableProcesses = JDBCUtil.queryList(connection, "select * from table_process", TableProcess.class, true);

        for (TableProcess tableProcess : tableProcesses) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkExtend());

            tableProcessHashMap.put(tableProcess.getSourceTable(),tableProcess);
        }
        connection.close();
    }


    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

        // 0.0 如果当前为删除数据操作,也应该在状态中进行删除
        // 先将value<String>转换为JsonObject
        JSONObject jsonObject = JSONObject.parseObject(value);
        // 难点:如何将DimApp中定义的变量MapStateDescriptor<String, TableProcess> stateDesc 传入到这里
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDesc);

        // 如果头里面的body存储信息是 d 的话 要将这条数据删掉
        if ("d".equals(jsonObject.getString("op"))) {
            String key = jsonObject.getJSONObject("before").getString("source_table");// 注意是从before中取数据 after中无数据
            // key 用到的是表名
            broadcastState.remove(key);
            // 如果这条数据删掉的话 在HashMap 中也要删掉
            tableProcessHashMap.remove(key);
        }else {
            String key = jsonObject.getJSONObject("after").getString("source_table");// 注意是从before中取数据 after中无数据
            // 要的是TableProcess 能提供是String的value,如何处理?
            // 答案: 利用JsonParse转换
            // 先将String转换为JsonObject
            // JSONObject jsonObject = JSONObject.parseObject(value);
            // JsonObject拿到其中的指定after头结构体信息
            String after = jsonObject.getString("after");
            // 再将after信息转换为TableProcess对象
            TableProcess tableProcess = JSONObject.parseObject(after, TableProcess.class);
            // 1.0 解析数据为TableProcess对象
            // 如果有删除数据的操作也应该在状态中删除该数据
            // 以上已完成

            // 2.0 检验并建表
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkPk(), // 获取主键
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkExtend()
            );

            // 3.0 将数据写入状态
            broadcastState.put(key,tableProcess);
        }

    }

    // 校验并建表: create table if not exists db.tn(id varchar primary key, name varchar, sex varchar) xxx;
    private void checkTable(String sinkTable, String sinkPk, String sinkColumns, String sinkExtend) {
        // 防止主键以及扩展字段出现null值 ,需要提前做判断
        if (sinkPk == null || "".equals(sinkPk)) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        // 拼接建表SQL语句
        StringBuilder sql = new StringBuilder("create table if not exists ")
                .append(GmallConfig.PHOENIX_DB)
                .append(".")
                .append(sinkTable)
                .append("(");
        String[] fields = sinkColumns.split(","); // "sink_columns": "id,sku_id,sku_name,busi_date,is_lastest,sku_cost,create_time"
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
            // 要判断这个字段是不是主键,如果是主键的话要特别声明
            sql.append(field).append(" varchar ");
            if (sinkPk.equals(field)) {
                // 证明是主键
                sql.append(" primary key");
            }
            // 判断是否为最后一个字段 ,如果不是最后一个字段,则需要添加","
            if (i != fields.length - 1) { // i != fields.length -1
                // 证明不是最后一个元素,需要添加逗号
                sql.append(",");
            }
        }
        sql.append(" ) ");

        // 打印sql语句
        System.out.println(sql);

        // 获取链接
        DruidPooledConnection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = druidDataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表: " + sinkTable + "失败!");
        } finally {

            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 1.0 读取状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDesc);
        // 数据是由MaxWell采集得来
        String key = value.getString("table");
        // 从配置表流状态中得到想要的配置表信息,对主流中的表进行过滤
        TableProcess tableProcess = broadcastState.get(key);
        // 从HashMap 中获取
        TableProcess tableProcessMap = tableProcessHashMap.get(key);

        // 2.0 过滤数据(行,列) 问题: 什么条件下存在行过滤?这一行不要
        // 因为主流表中采集的topic_db 中的表数据有各种各样的,其中包含事实表的数据,如果有事实表的表进行查找状态的话,就会是null
        JSONObject data = value.getJSONObject("data");
        // 由于是maxwell采集 需要过滤掉开头以及结尾的脏数据  data字段为null 即为bootstrap-start /bootstrap-complete
        if ((tableProcess != null || tableProcessMap != null) && data != null) { //<-- 按照行过滤
        // 3.0 将过滤后的数据输出
            if (tableProcess == null) {
                 tableProcess =  tableProcessMap;
            }
            // 按照列过滤 主流中的JSONObject 中的data字段
            filterColumns(data,tableProcess.getSinkColumns());
            value.put("sink_table",tableProcess.getSinkTable());
            out.collect(value);
        }else{
            System.out.println("没有对应: " +key +"的配置信息");
        }


    }
    // 根据建表字段过滤主流数据字段
    // data{"id":"12","tm_name":"lenovo","logo_url":"xxxxx"} -- > 主流中的数据
    //sinkColumns:id,tm_name --> 过滤要求字段
    private void filterColumns(JSONObject data, String sinkColumns) {
        // 遍历这个JsonObject,把不用的字段删掉
        String[] fields = sinkColumns.split(",");
        List<String> fieldList = Arrays.asList(fields);
        Set< Map.Entry<String, Object>> entries = data.entrySet();
/*        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            if (!fieldList.contains(entry.getKey())) {
                iterator.remove();
            }
        }*/
        entries.removeIf(entry -> !fieldList.contains(entry.getKey()));

    }


}
















