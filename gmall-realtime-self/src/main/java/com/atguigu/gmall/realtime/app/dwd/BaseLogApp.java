package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// 数据流: web/app -> 日志服务器(log文件) -> flume1 -> kafka(ODS) -> FlinkAPP -> Kafka(DWD)
// 程序: Mock -> 文件 -> f1.sh -> Kafka(ZK) -> BaseLogAPP -> Kafka(ZK)

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        // todo 1.0 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // -->此处设置为1 是因为topic_db只有1个分区 生产环境有几个分区就设置几个分区
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
        // todo 2.0 读取Kafka topic_log主题数据创建流
        String topic = "topic_log";
        // 问题: 为什么要设置不同的消费者组?
        // 回答: 因为我们如果设置同一个groupID ,那么我们相当于跟Dim在同一个消费者组中,那么我们消费到的数据就不是全量的数据 ,而是部分的数据信息,
        //      因此我们要设置不同的groupID 组使得我们这个dwd层消费到的topic_db是全量的数据
        String groupID = "base_log_app_220828";
        DataStreamSource<String> kfDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupID));

        // todo 3.0 将数据转换为Json对象,并过滤掉非JSON数据
        // 搞一个侧输出流对象,使得脏数据输出到测输出流中 --> 为什么是String 而不是Json类型? --> 未能够成功转换为Json 原来的类型是String类型
        OutputTag<String> invalidDatas = new OutputTag<String>("invalidDatas", TypeInformation.of(String.class));
        SingleOutputStreamOperator<JSONObject> jsonDS = kfDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                    // 难点: 要拿到这条脏数据
                    ctx.output(invalidDatas, value);
                }
            }
        });

        // 提取测输出流中的数据并打印
        jsonDS.getSideOutput(invalidDatas).print();

        // todo 4.0 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonDS.keyBy((KeySelector<JSONObject, String>) value -> (value.getJSONObject("common").getString("mid")));

        // todo 5.0 将数据进行新老用户标记修复
        // 使用map方法
        SingleOutputStreamOperator<JSONObject> jsonObjectWithNewFlag = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                // 提取状态数据以及当前数据中的is_new标记,提取数据中的时间错生成日期
                String lastVisitDt = lastVisitDtState.value();
                String isNew = value.getJSONObject("common").getString("is_new");
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);

                if ("1".equals(isNew)) {
                    if (lastVisitDt == null) {
                        // 说明该数据为第一天的第一条数据 ,数据保持不变,状态更新为当日
                        lastVisitDtState.update(curDt);
                    } else if (!lastVisitDt.equals(curDt)) {
                        // 说明该数据不是第一天的数据 且中间有卸载,那么就休要将标记改为0
                        value.getJSONObject("common").put("is_new", "0");
                    }
                } else if (lastVisitDt == null) {
                    // 说明该数据在任务启动之前就有访问的数据, 所以将状态更新为1970-01-01
                    lastVisitDtState.update("1970-01-01");
                }
                return value;
            }
        });

        // todo 6.0 使用测输出流对数据进行分流处理一个主流 四个测输出流 页面日志放到主流 其他日志放到侧输出流
        OutputTag<String> startTag = new OutputTag<>("start", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<>("display", TypeInformation.of(String.class));
        OutputTag<String> actionTag = new OutputTag<>("action", TypeInformation.of(String.class));
        OutputTag<String> errorTag = new OutputTag<>("error", TypeInformation.of(String.class));

        SingleOutputStreamOperator<String> pageDS = jsonObjectWithNewFlag.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {

                // 尝试取出错误字段
                String err = value.getString("err");
                if (err != null && !err.equals("")) {
                    // 该数据中包含错误信息
                    ctx.output(errorTag, value.toJSONString());
                    // 将该字段删掉,因为我们不需要在页面中保留该字段信息
                    value.remove("err");

                }

                // 尝试获取启动日志
                String start = value.getString("start");
                if (start != null && !"".equals(start)) {
                    // 该数据为启动日志
                    ctx.output(startTag, value.toJSONString());
                } else {
                    // 该数据为页面日志

                    // 提取页面ID 与时间戳以及公共字段
                    String pageID = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");
                    JSONObject common = value.getJSONObject("common");


                    // 尝试获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    // 问题: 为什么要判断为数组为null?
                    // 回答: 有些公司就是要保留这个displays这个标签 有可能只是保留这个标签 但是里面没有数据因此要再次进行判断里面数组的个数>0的情况
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageID);
                            display.put("ts", ts);
                            display.put("common", common);

                            // 输出数据
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                    // 尝试获取动作数据
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("page_id", pageID);
                            action.put("ts", ts);
                            action.put("common", common);

                            // 输出数据
                            ctx.output(actionTag, action.toJSONString());
                        }
                    }
                    // 页面日志
                    value.remove("displays");
                    value.remove("actions");
                    out.collect(value.toJSONString());
                }
            }
        });


        // todo 7.0 提取测输出流并将数据写出到对应的Kafka主题
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);

        pageDS.print("pageDS >>>>");
        startDS.print("startDS >>>>");
        displayDS.print("displayDS >>>>");
        actionDS.print("actionDS >>>>");
        errorDS.print("errorDS >>>>");

        // 7.2 定义不同日志输出到 Kafka 的主题名称
        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";


        // 增加sink 可以定义一个FlinkFunction
        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(start_topic));
        displayDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(action_topic));
        errorDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(error_topic));

        // todo 8.0 启动任务

        env.execute("BaseLogApp");
    }
}
