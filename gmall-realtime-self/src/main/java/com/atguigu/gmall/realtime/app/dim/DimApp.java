package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DIM_MyBroadcastFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DimApp {
    public static void main(String[] args) {
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

        // todo 2.0 过滤topic_db 中的数据并转换为JSON 格式(保留新增 变化 以及初始化数据)
        SingleOutputStreamOperator<JSONObject> jsonObjDS
                = kfDS.flatMap((FlatMapFunction<String, JSONObject>) (value, out) -> {
            if (value != null) {
                try {
                    // 将数据转为JSON格式
                    JSONObject jsonObject = JSON.parseObject(value);
                    // 获取数据中的操作类型字段
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
        });

        // todo 3.0 使用FlinkCDC 读取配置信息表
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
        coStream.process(new DIM_MyBroadcastFunction(stateDesc));

        // todo 7.0 将数据写出到Phoenix

        // todo 8.0 启动任务


    }
}
