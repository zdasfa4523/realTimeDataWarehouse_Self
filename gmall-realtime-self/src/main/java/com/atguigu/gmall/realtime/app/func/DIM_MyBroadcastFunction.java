package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.google.gson.JsonObject;
import io.debezium.data.Json;
import javafx.scene.control.Tab;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class DIM_MyBroadcastFunction extends BroadcastProcessFunction<JSONObject, String, JsonObject> {

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

    @Override
    public void open(Configuration parameters) throws Exception {
        // 创建链接
    }

    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JsonObject>.Context ctx, Collector<JsonObject> out) throws Exception {

        // 0.0 如果当前为删除数据操作,也应该在状态中进行删除
        // 先将value<String>转换为JsonObject
        JSONObject jsonObject = JSONObject.parseObject(value);
        // 难点:如何将DimApp中定义的变量MapStateDescriptor<String, TableProcess> stateDesc 传入到这里
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDesc);
        String removeKey = jsonObject.getJSONObject("after").getString("source_table");
        // 如果头里面的body存储信息是 d 的话 要将这条数据删掉
        if ("d".equals(jsonObject.getString("op"))) {
            // key 用到的是表名
            broadcastState.remove(removeKey);
        }
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
        StringBuilder stringBuilder = new StringBuilder("create table if not exists ")
                .append(GmallConfig.PHOENIX_DB)
                .append(".")
                .append(sinkTable)
                .append("(");
        String[] fields = sinkColumns.split(","); // "sink_columns": "id,sku_id,sku_name,busi_date,is_lastest,sku_cost,create_time"
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
            // 要判断这个字段是不是主键,如果是主键的话要特别声明
            stringBuilder.append(field).append(" varchar");
            if (sinkPk.equals(field)) {
                // 证明是主键
                stringBuilder.append(field).append(" primary key");
            }
            // 判断是否为最后一个字段 ,如果不是最后一个字段,则需要添加","
            if (i < fields.length - 1) { // i != fields.length -1
                // 证明不是最后一个元素,需要添加逗号
                stringBuilder.append(",");
            }
        }
        stringBuilder.append(" ) ")
                .append(sinkColumns);

        // 打印sql语句
        System.out.println(stringBuilder);

    }


    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JsonObject>.ReadOnlyContext ctx, Collector<JsonObject> out) throws Exception {
        // 1.0 读取状态
        // 2.0 过滤数据(行,列)
        // 2.0 将过滤后的数据输出


    }


}
