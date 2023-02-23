package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyClickhouseUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Dws03TrafficPageViewWindow {
    public static void main(String[] args) throws Exception {
        //1 获取流的执行环境
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

        //2 读取Kafka页面日志主题数据创建流
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("dwd_traffic_page_log", "page_view_220828"));

        //3 将数据转换为 Json对象
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String pageID = jsonObject.getJSONObject("page").getString("page_id");

                if ("home".equals(pageID) || "good_detail".equals(pageID)) {
                    out.collect(jsonObject);
                }


            }
        });

        //5. 提取时间戳生成watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWMDS = jsonDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //6. 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWMDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //7. 去重得到首页以及商品详情页的独立访客转换为javabean对象

        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> homeDetailPageViewDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {


            // 编写两个状态 存放 商品详情以及首页状态日期
            private ValueState<String> homeLastDtState;
            private ValueState<String> detailLastDtState;

            @Override
            public void open(Configuration parameters) throws Exception {

                // 状态过期时间设置
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();


                // 初始化状态
                ValueStateDescriptor<String> homeDesc = new ValueStateDescriptor<>("home-dt", String.class);
                homeDesc.enableTimeToLive(ttlConfig);
                ValueStateDescriptor<String> detailDesc = new ValueStateDescriptor<>("detail-dt", String.class);
                detailDesc.enableTimeToLive(ttlConfig);

                homeLastDtState = getRuntimeContext().getState(homeDesc);
                detailLastDtState = getRuntimeContext().getState(detailDesc);
            }


            @Override
            public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                // 获取状态数据以及当前数据中的日期
                String lasthomeDt = homeLastDtState.value();
                String lastDetailDt = detailLastDtState.value();

                //获取当前日期
                String curDt = DateFormatUtil.toDate(value.getLong("ts"));


                //获取页面ID
                String pageID = value.getJSONObject("page").getString("page_id");
                // 判断是哪种页面?
                //定义变量
                long homeCt = 0l;
                long detailct = 0l;

                if ("home".equals(pageID)) {
                    // 说明是首页 拿状态日期跟当前日期做比较
                    if (lasthomeDt == null || !lasthomeDt.equals(curDt)) {
                        homeCt = 1l;
                        homeLastDtState.update(curDt);
                    }
                } else {
                    if (lastDetailDt == null || !lastDetailDt.equals(curDt)) {
                        detailct = 1l;
                        detailLastDtState.update(curDt);
                    }
                }

                //todo  编写过滤条件 只要带有数据的

                if (homeCt == 1l || detailct == 1l) {
                    out.collect(new TrafficHomeDetailPageViewBean("", "", homeCt, detailct, 0L));
                }


            }


        });

        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultDS = homeDetailPageViewDS
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {

                        //获取数据
                        TrafficHomeDetailPageViewBean next = values.iterator().next();

                        //补充信息
                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                        //输出数据
                        out.collect(next);
                    }
                });


        //TODO 8.将数据写出到ClickHouse
        resultDS.print(">>>>>>");
        resultDS.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));

        //TODO 9.启动任务
        env.execute("Dws03TrafficPageViewWindow");


    }


}

