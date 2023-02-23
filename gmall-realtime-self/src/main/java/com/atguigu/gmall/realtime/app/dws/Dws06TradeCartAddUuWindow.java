package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.CartAddUuBean;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyClickhouseUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

public class Dws06TradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {

//    todo    从 Kafka 读取用户加购明细数据，统计各窗口加购独立用户数，写入 ClickHouse。

        // 去重 每日  日期为null 或者不等于当天日期
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // todo 读取kafka数据
        String topic = "dwd_trade_cart_add";

        DataStreamSource<String> kfDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, "Dws06TradeCartAddUuWindow_03"));

        //todo 将对象转换为JSON 对象
        SingleOutputStreamOperator<JSONObject> jsonDS = kfDS.map(JSONObject::parseObject);

        //todo 提取时间时间生成watermark
        SingleOutputStreamOperator<JSONObject> jsonWithwmDS = jsonDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                String operateTime = element.getString("operate_time");
                if (operateTime != null) {
                    return DateFormatUtil.toTs(operateTime, true);
                } else {
                    System.out.println("Ts:::::::"+DateFormatUtil.toTs(element.getString("create_time"),true));
                    return DateFormatUtil.toTs(element.getString("create_time"),true);
                }

            }
        }));

        jsonDS.print("jsonDS>>>>>>>>>>>>>>>>");


        //todo 按照User_id分组
        KeyedStream<JSONObject, String> keyedStream = jsonWithwmDS.keyBy(json -> json.getString("user_id"));

        //todo 使用状态编程提取独立加购用户
        SingleOutputStreamOperator<CartAddUuBean> cartAddDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {

            private ValueState<String> lastCartAddState;

            @Override
            public void open(Configuration parameters) throws Exception {

                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();


                ValueStateDescriptor<String> last_dateDesc = new ValueStateDescriptor<>("LAST_DATE", String.class);
                lastCartAddState = getRuntimeContext().getState(last_dateDesc);
            }

            @Override
            public void flatMap(JSONObject value, Collector<CartAddUuBean> out) throws Exception {
                // 获取状态数据以及当前数据的日期
                // 取出状态中这个user_id中保存的上次的加购日期
                String lastDt = lastCartAddState.value();
                // 获取这个user_id当前的访问时间跟状态中的日期做对比

                String operaTime = value.getString("operate_time");
                String curDt = null;
                if (operaTime != null) {
                    curDt = operaTime.split(" ")[0];
                }else{
                    String create_time = value.getString("create_time");
                    curDt = create_time.split(" ")[0];
                }

                // 如果状态为null 或者日期不一致 则算作一个 头一个数据 就要这条数据
                if (lastDt == null || !lastDt.equals(curDt)) {
                    lastCartAddState.update(curDt);
                    out.collect(new CartAddUuBean("", "", 1L, null));
                }
            }
        });

        cartAddDS.print("cartAddDS>>>>>>>>>>>>>>>");

        //todo 开窗聚合
        SingleOutputStreamOperator<CartAddUuBean> reduceDS = cartAddDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                            @Override
                            public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                                value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());

                                return value1;
                            }
                        },
                        new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {
                                CartAddUuBean next = values.iterator().next();

                                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                                next.setTs(System.currentTimeMillis());

                                out.collect(next);
                            }
                        });

        //todo 将数据写出到clickhouse
        reduceDS.print(">>>>>>");
        reduceDS.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));

        //启动执行环境

        env.execute();

    }
}
