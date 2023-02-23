package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.JDBCUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class DWD_MyBroadcastFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    MapStateDescriptor<String, TableProcess> mapStateDesc;
    HashMap<String, TableProcess> tableProcessHashMap;

    public DWD_MyBroadcastFunction(MapStateDescriptor<String, TableProcess> mapStateDesc) {
        this.mapStateDesc = mapStateDesc;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        tableProcessHashMap = new HashMap<>();
        // 通过jdbc读取配置表信息赋值到tableProcessHashMap中
        Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall-config-0828?" +
                "user=root&password=123456&useUnicode=true&" +
                "characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false");
        List<TableProcess> tableProcesses = JDBCUtil.queryList(connection, "select * from table_process where sink_type='dwd'", TableProcess.class, true);
        //遍历赋值
        for (TableProcess tableProcess : tableProcesses) {
            System.out.println("Open:" + tableProcess.getSourceTable() + "-" + tableProcess.getSourceType());
            tableProcessHashMap.put(tableProcess.getSourceTable() + "-" + tableProcess.getSourceType(), tableProcess);
        }
    }


    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        /*
         * FlinkCDC读取数据:
         * {
         *     "before":null,
         *     "after":{
         *         "source_table":"base_trademark",
         *         "sink_table":"dim_base_trademark",
         *         "sink_columns":"id,tm_name",
         *         "sink_pk":"id",
         *         "sink_extend":null
         *     },
         *     "source":{
         *         "version":"1.5.4.Final",
         *         "connector":"mysql",
         *         "name":"mysql_binlog_source",
         *         "ts_ms":1655172926148,
         *         "snapshot":"false",
         *         "db":"gmall-211227-config",
         *         "sequence":null,
         *         "table":"table_process",
         *         "server_id":0,
         *         "gtid":null,
         *         "file":"",
         *         "pos":0,
         *         "row":0,
         *         "thread":null,
         *         "query":null
         *     },
         *     "op":"r",
         *     "ts_ms":1655172926150,
         *     "transaction":null
         * }
         */
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDesc);
        JSONObject jsonObject = JSONObject.parseObject(value);
        String op = jsonObject.getString("op");
        // dwd中的状态是怎么定义出来的key? 确认key的话就要确认唯一性 ,如果业务中不需要太细的粒度,就不需要确认相关的确认事实类型的操作
        // 例如 用户领取优惠券之后使用 就是两个事实 那么两个事实两个事实粒度就不能往同一个事实表中操作,一个是用户做了领取,一个是用户做了使用,更新了之前的信息因此就要
        // 用表名 + 操作类型对事实进行细细划分,划分到两个不同的topic主题中去

        if ("d".equals(op)) {
            // 说明cdc读取到的是将配置流的行(配置表)进行删除,因此需要将状态中保存的配置状态也进行删除
            TableProcess before = JSONObject.parseObject(jsonObject.getString("before"), TableProcess.class);
            String key = before.getSourceTable() + "-" + before.getSourceType();
            // 删除状态
            broadcastState.remove(key);
            tableProcessHashMap.remove(key);
        } else {
            // 说明读取到的cdc有可能是新增或者修改操作,放入状态即可
            TableProcess after = JSONObject.parseObject(jsonObject.getString("after"), TableProcess.class);
            String key = after.getSourceTable() + "-" + after.getSourceType();
            // 将数据存放到状态中去

            System.out.println("processBroadcastElement:" + key);

            broadcastState.put(key, after);
        }
    }


    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
/* Maxwell读取到存储在Kafka中的数据信息:
        {
            "database":"gmall",
                "table":"cart_info",
                "type":"update",
                "ts":1592270938,
                "xid":13090,
                "xoffset":1573,
                "data":{
                    "id":100924,
                    "user_id":"93",
                    "sku_id":16,
                    "cart_price":4488,
                    "sku_num":1
                    },
            "old":{
            "is_ordered":0,
                    "order_time":null
        }
        }*/

        // 得到状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDesc);
        // 此处的value就是个jsonObject
        String key = value.getString("table") + "-" + value.getString("type");

        System.out.println("processElement:" + key);

        TableProcess tableProcess = broadcastState.get(key);
        // 除了要在状态中匹配,也要在HashMap中匹配相关数据
        TableProcess tableProcessMap = tableProcessHashMap.get(key);

        // 根据返回的tableProcess来判断是不是要过滤的topic信息表
        if (tableProcess != null || tableProcessMap != null) {
            // 说明就是我们要的状态信息 ,将这条value返回到主流中进行sink输出
            if (tableProcess == null) {
                tableProcess = tableProcessMap;
            }
            // 过滤行信息
            filterColumns(value.getJSONObject("data"), tableProcess.getSinkColumns());
            JSONObject dataObject = value.getJSONObject("data");
            // todo 需要添加列值sink_table字段信息吗?
            dataObject.put("sink_table", tableProcess.getSinkTable());
            System.out.println("eleprocess要输出的数据为:" + dataObject.toString());
            // 将Maxwell提取过滤完的数据直接返回到主流中
            out.collect(dataObject);


        }
    }

    private void filterColumns(JSONObject data, String sinkColumns) {
        // id,name,age(主流) --> id,name(配置流字段)
        String[] split = sinkColumns.split(",");
        List<String> columns = Arrays.asList(split); // 配置表列

        Set<String> keys = data.keySet(); // 主流数据中的列
        // 如果在配置表sinkColumns中无法匹配到 主流中的键的话就删除主流中无法匹配的key/value信息
        keys.removeIf(key -> !columns.contains(key));
    }
}


















