package com.atguigu.gmall.realtime.app.dwd.db;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DWD_MyBroadcastFunction;
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
import org.apache.flink.util.Collector;

public class BaseDBApp {
    /**
     * todo DWD层余下的事实表都是从topic_db中取业务数据库一张表的变更数据，
     * 按照某些条件过滤后写入Kafka的对应主题，
     * 它们处理逻辑相似且较为简单，可以结合配置表动态分流在同一个程序中处理。
     *
     */

    public static void main(String[] args) throws Exception {
        // 1.0 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
        // 2.0 获取topic_db中的数据
        // env.addsource()
        DataStreamSource<String> KafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("topic_db", "BaseDBApp_0828_01"));

        // 3.0 对topic_db中的数据字段做JsonObject转换处理  主流
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = KafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                // 判断从kafka中传入的数据是否为null --> UpsertKafka 会产生Null值信息
                if (value != null) {
                    // 转换为JsonObject 对象信息

                    try {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        // todo 输出到主流中
                        out.collect(jsonObject);
                    } catch (Exception e) {
                        System.out.println("错误的脏信息 : >>" + value);
                    }
                }
            }
        });

        // 4.0 读取配置 配置流信息 并配置为广播流信息
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .username("root")
                .password("123456")
                .port(3306)
                .databaseList("gmall-config-0828")
                .tableList("gmall-config-0828.table_process")
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mysqlDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysqlDS");
        MapStateDescriptor<String, TableProcess> mapStateDesc = new MapStateDescriptor<String, TableProcess>("dwdstatedesc",String.class,TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlDS.broadcast(mapStateDesc);

        // 5.0 处理两条流合并信息
        BroadcastConnectedStream<JSONObject, String> connectDS = jsonObjectDS.connect(broadcastStream);

        // 6.0 处理合并流信息
        SingleOutputStreamOperator<JSONObject> resultDS = connectDS.process(new DWD_MyBroadcastFunction(mapStateDesc));



        // 7.0 写入到Kafka中
        resultDS.addSink(MyKafkaUtil.getFlinkKafkaProducer());

        // 8.0 执行环境
        env.execute();
    }
}
