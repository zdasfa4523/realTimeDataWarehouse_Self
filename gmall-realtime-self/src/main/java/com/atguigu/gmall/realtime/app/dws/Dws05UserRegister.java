package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.UserRegisterBean;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyClickhouseUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class Dws05UserRegister {
    public static void main(String[] args) throws Exception {
        // todo 从 DWD 层用户注册表中读取数据，统计各窗口注册用户数，写入 ClickHouse。

        //1 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //        // 需要从Checkpoint或者Savepoint启动程序
//      //开启Checkpoint,每隔5秒钟做一次CK  ,并指定CK的一致性语义
//      env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//      //设置超时时间为 1 分钟
//      env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//      //设置两次重启的最小时间间隔
//      env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//      //设置任务关闭的时候保留最后一次 CK 数据
//      env.getCheckpointConfig().enableExternalizedCheckpoints(
//              CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//      //指定从 CK 自动重启策略
//      env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1L), Time.minutes(1L)));
//      //设置状态后端
//      env.setStateBackend(new HashMapStateBackend());
//      env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flinkCDC/220828");
//      //设置访问HDFS的用户名
//      System.setProperty("HADOOP_USER_NAME", "atguigu");

        DataStreamSource<String> kfDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("dwd_user_register", "Dws05UserRegister_082801"));

        // 2.0 从kf注册的信息中转换为UserRegisterBean
/*        {
            "create_time": "2022-02-21 17:59:15",
                "id": 1600
        }*/

        // 由于dwd是insert操作 所以不需要考虑去重问题直接统计即可
        SingleOutputStreamOperator<UserRegisterBean> userRegDS = kfDS.map(new RichMapFunction<String, UserRegisterBean>() {
            @Override
            public UserRegisterBean map(String value) throws Exception {
                Long ts = null;

                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);

                    String create_time = jsonObject.getString("create_time");

                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date datetime = simpleDateFormat.parse(create_time);

                    ts = datetime.getTime();


                } catch (ParseException e) {
                    System.out.println(ts);
                    System.out.println("转换有误 : " + value);
                }

                return new UserRegisterBean("", "", 1l, ts);

            }
        });

        SingleOutputStreamOperator<UserRegisterBean> waterMarkDS = userRegDS.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner((word, kk) -> word.getTs()));

        // 4.0 开窗聚合
        SingleOutputStreamOperator<UserRegisterBean> resultDS = waterMarkDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                            @Override
                            public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {

                                value1.setUserRegCt(value1.getUserRegCt() + value2.getUserRegCt());



                                return value1;
                            }
                        }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<UserRegisterBean> values, Collector<UserRegisterBean> out) throws Exception {

                                // 获取数据
                                UserRegisterBean next = values.iterator().next();
                                // 补充窗口信息
                                next.setTs(System.currentTimeMillis());
                                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));


                                System.out.println("next >>> "+next);
                                out.collect(next);
                            }
                        }
                );


        // 5.0 输出到Clickhouse
        resultDS.print("result>>>");
        resultDS.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_user_user_register_window values(?,?,?,?)"));

        // 5.0 执行流环境
        env.execute();


    }
}
