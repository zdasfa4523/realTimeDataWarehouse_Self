package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DIM_MyBroadcastFunction;
import com.atguigu.gmall.realtime.app.func.DIM_SinkFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DimApp {
    // 数据流:web/app -> Mysql(binlog) -> Maxwell -> Kafka (ODS) -> FlinkApp -> Phoenix
    //程序: Mock -> Mysql(binlog)-> Maxwell -> Kafka (zk) -> DimApp(HDFS,ZK,HBASE) -> Phoenix(DIM)

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // -->此处设置为1 是因为topic_db只有1个分区 生产环境有几个分区就设置几个并行度数据
/*
        // 生产环境必须配置 !!!!
        // 开启CheckPoint机制
        env.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        // 开启状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flinkDW_self/ck");
        // 设置HADOOP权限
        System.setProperty("HADOOP_USER_NAME","atguigu");
*/
        // todo 1.0 主流消费Kafka中 topic_db主题的数据
        DataStreamSource<String> kfDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("topic_db", "dim_app_01"));

        // todo 2.0 过滤topic_db 中的数据并转换为JSON 格式(保留新增 变化 以及初始化数据) 为什么要转为JsonObject格式 而不是转为一个JavaBean?
        // Json 对比 JavaBean 有什么优势?
        // 回答: JavaBean有直接的属性名称 可以直接调用  Json无需提前声明 可以通过GetString()来得到相应的KV信息
        // 对于new OutTag()进行测输出流的数据  除了可以写匿名内部类的方式 还可以在形参内部指定TYPEInformation,避免泛型擦除问题
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kfDS.flatMap((FlatMapFunction<String, JSONObject>) (String value, Collector<JSONObject> out) -> {
            if (value != null) { // 避免空值
                try {
                    // 将topic_dbString类型的数据转为JSON格式
                    JSONObject jsonObject = JSON.parseObject(value);
                    // 获取数据中的操作类型字段
                    // Maxwell中的数据类型:
                    // {"database":"gmall","table":"cart_info","type":"update","ts":1592270938,"xid":13090,"xoffset":1573,"data":{"id":100924,"user_id":"93","sku_id":16,"cart_price":4488,"sku_num":1,"img_url":"http://47.93.148.192:8080/group1/M0rBHu8l-sklaALrngAAHGDqdpFtU741.jpg","sku_name":"华为 HUAWEI P40 麒麟990 5G SoC芯片 5000万30倍数字变焦 8GB+128GB亮黑色全网通5G手机","is_checked":null,"create_time":"2020-06-14 09:28:57","operate_time":null,"is_ordered":1,"order_time":"2021-10-17 09:28:58","source_type":"2401","source_id":null},"old":{"is_ordered":0,"order_time":null}}
                    String type = jsonObject.getString("type");
                    //保留新增 变化 以及初始化数据 这一块为什么不要了?
                    if ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type)) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("非JSON格式数据 : " + value);
                }
            }
        }, TypeInformation.of(JSONObject.class));//  避免泛型擦除的问题

        // todo 3.0 使用FlinkCDC 读取配置信息表生成配置流数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-config-0828")
                .tableList("gmall-config-0828.table_process") // 也可以不写 因为这个库就有一张表
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        // 加载配置表Source流信息
        DataStreamSource<String> mysqlStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource");

        // todo 4.0 将配置信息流转换为广播流
        // --> MapStateDescriptor 中的泛型怎么确认?
        // 回答 对于这个MapStateDescriptor来说 ,因为是配置信息表 ,每一行中的k设置为表名,因此使用String即可 , V的话就是这一行的数据 即可用String 本次可以封装为一个JavaBean
        // 更进一部讲是将获取到的String类型转换为TableProcess类型
        MapStateDescriptor<String, TableProcess> stateDesc = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        // 为什么广播流的BroadcastStream中的泛型是<String>,而不是TableProcess? 因为我们获取到的 MySqlSource<String> mySqlSource 就是String类型
        // mysql配置流要广播出去
        BroadcastStream<String> broadcastStream = mysqlStreamSource.broadcast(stateDesc);

        // todo 5.0 链接主流(数据流)和广播流
        BroadcastConnectedStream<JSONObject, String> coStream = jsonObjDS.connect(broadcastStream);

        // todo 6.0 根据广播状态中的参数来处理(过滤)主流数据
        // 问题: 输出的泛型参数写什么?
        // 回答: 我们这个输出要做什么? 要的是经过主流的数据过滤之后(原来四十多张表过滤之后只剩下维表数据) 数据类型 因此是JsonObject
        SingleOutputStreamOperator<JSONObject> hbaseDS = coStream.process(new DIM_MyBroadcastFunction(stateDesc));

        // todo 7.0 将数据写出到Phoenix
        hbaseDS.print(">>>>>>>");
        hbaseDS.addSink(new DIM_SinkFunction());
        // todo 8.0 启动任务
        env.execute("DimAPP");


    }
}
