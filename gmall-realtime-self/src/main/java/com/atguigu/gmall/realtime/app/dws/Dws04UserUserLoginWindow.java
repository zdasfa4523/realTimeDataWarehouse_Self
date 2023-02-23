package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.UserLoginBean;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyClickhouseUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Dws04UserUserLoginWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //        // 需要从Checkpoint或者Savepoint启动程序
//        //开启Checkpoint,每隔5秒钟做一次CK  ,并指定CK的一致性语义
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        //设置超时时间为 1 分钟
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        //设置两次重启的最小时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        //设置任务关闭的时候保留最后一次 CK 数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //指定从 CK 自动重启策略
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1L), Time.minutes(1L)));
//        //设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flinkCDC/220828");
//        //设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.读取Kafka dwd层 页面日志主题数据
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("dwd_traffic_page_log", "user_login_220828"));

        //TODO 3.过滤数据&转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String uid = jsonObject.getJSONObject("common").getString("uid");
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");

                if (uid != null && (lastPageId == null || "login".equals(lastPageId))) {
                    out.collect(jsonObject);
                }
            }
        });

        //TODO 4.提取时间戳生成WaterMark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWMDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //TODO 5.按照UID进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWMDS.keyBy(json -> json.getJSONObject("common").getString("uid"));

        
        //todo 去重数据转换为JavaBean对象
        SingleOutputStreamOperator<UserLoginBean> userloginDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {

            private ValueState<String> lastLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastLoginDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-login", String.class));
            }


            @Override
            public void flatMap(JSONObject value, Collector<UserLoginBean> out) throws Exception {

                // 获取状态中的数据以及当前数据中的日期
                String lastDt = lastLoginDtState.value();
                String curDt = DateFormatUtil.toDate(value.getLong("ts"));

                Long uv = 0L;
                Long backCt = 0l;

                // 定义当天用户登陆书 以及回流用户数
                if (lastDt == null) {
                    uv = 1l;
                    lastLoginDtState.update(curDt);
                } else if (lastDt.compareTo(curDt) < 0) {
                    uv = 1l;
                    lastLoginDtState.update(curDt);

                    // 判断是否为回流用户
                    if ((DateFormatUtil.toTs(curDt) - (DateFormatUtil.toTs(lastDt))) / (24 * 3600 * 1000L) > 7) {
                        backCt = 1l;
                    }


                }

                // 将数据写出
                if (uv == 1L) {
                    out.collect(new UserLoginBean("", "", backCt, uv, 0L));
                }


            }
        });

        //TODO 7.开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resultDS = userloginDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {

                        UserLoginBean next = values.iterator().next();

                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                        out.collect(next);
                    }
                });

        //TODO 8.将数据写出
        resultDS.print(">>>>>>");
        resultDS.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_user_user_login_window values(?,?,?,?,?)"));

        //TODO 9.启动任务
        env.execute("Dws04UserUserLoginWindow");


    }
}
