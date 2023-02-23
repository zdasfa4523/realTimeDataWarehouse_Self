package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyClickhouseUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Dws02TrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {

        //DWS 层是为 ADS 层服务的，通过对指标体系的分析，本节汇总表中需要有会话数、页面浏览数、浏览总时长和独立访客数四个度量字段。
        // 会话数 LAST_PAGE_ID = NULL
        // 页面浏览数 来一条数据直接累加 +1
        // 浏览总时长 DURING_TIME 直接做累加
        // 独立访客数 求之前做去重 --> 先按mid做分组,做数据去重


        // todo 本节的任务是统计这四个指标，并将维度和度量数据写入 ClickHouse 汇总表。
        //流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表

        //1.0 创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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
        //2.0 读取Kafka中的数据信息 topic_log
        DataStreamSource<String> kfDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("dwd_traffic_page_log", "vcchanew_page)"));
        //3.0 转换为JSON 对象
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kfDS.flatMap(new RichFlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                out.collect(jsonObject);
            }
        });

        //4.0 将数据按照mid进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDS.keyBy(bean -> bean.getJSONObject("common").getString("mid"));


        //5.0 计算各个度量值 转换数据结构为JavaBean
        // 每一条数据都对应一个JavaBean 计算之前独立访客去重 应该用状态变成
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewBean = keyedStream.map(new RichMapFunction<JSONObject, TrafficPageViewBean>() {
            // 声明状态 主要是为了去重
            // 为了独立访客 ,每个mid 中都需要独立计算 例如13206 这个mid 今天上午进行了访问 下午有进行了访问 但是日活的话只能算一次 下午的访问不能算 ,因此需要状态变成  看这个mid 之前是否来过 --> 状态里面存放什么能够保证这个mid是唯一的,假设存放的是ts 那么上午下午的访问事件不一样 因此 并不能达到去重的效果 因此可以考虑存放日期类型的字符串 当这个mid在状态中寻找时,发现 之前的状态为null ,说明是新访客 ,算做独立用户访问,也就是日活,并将当日的日期更新到状态中 ,这样即使下午再次访问,两次的日期相同 ,也算作一条的数据 //todo 另外 假设是第二天来的数据 ,状态中的日期是前一天,那么相当于第二天的第一条数据 ,  我们也要将这条数据算作独立访客 ,不更新状态中的日期的话好像没什么影响?
            // 答案: 其实不对 如果简单的判断今天的日期和状态中保存的日期不相等 便算作独立访客 ,但是不更新日期的话 那么第二天的下午再次登录 ,状态中存储的依然是昨天的日期 而不是今天的日期 因此还会计算一遍日活 相等于第二天计算了两次日活 因此 必须要更新到第二天的状态 才可以避免下午重复计算的情况
            // 声明状态 主要是为了去重
            private ValueState<String> lastvisitDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 这样写为什么飘红?
//                ValueState<T> state = getRuntimeContext().getState(lastvisitDtState);
                // ↑缺少状态描述信息
                ValueStateDescriptor<String> lastdtstateDesc = new ValueStateDescriptor<>("lastdtstate", String.class);
                // 在open方法中给状态初始化

                //设置过期时间
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                //对我们的状态描述器设置ttl过期时间以及更新策略
                lastdtstateDesc.enableTimeToLive(ttlConfig);

                lastvisitDtState = getRuntimeContext().getState(lastdtstateDesc);

            }

            @Override
            public TrafficPageViewBean map(JSONObject value) throws Exception {
                //先计算独立访问/日活  先得到
                String lastDt = lastvisitDtState.value();

                // 上面是mid的状态 需要跟自己的日期时间比较
                Long ts = value.getLong("ts");
                // 将时间戳转换为提起格式
                String curDt = DateFormatUtil.toDate(ts);
                // 日活的标准就是last_page_id 为null的数据
                // 获取数据中的last_page_id
                String lastPageID = value.getJSONObject("page").getString("last_page_id");

                // 获取版本信息
                String curVc = value.getJSONObject("common").getString("vc");
                // 获取渠道信息
                String curCh = value.getJSONObject("common").getString("ch");
                // 获取地区信息
                String curAr = value.getJSONObject("common").getString("ar");
                // 获取是否新老用户
                String isNew = value.getJSONObject("common").getString("is_new");

                // 获取独立访客数
                Long UVCt = 0L;
                if (lastDt == null || !lastDt.equals(curDt)) {
                    // 说明是独立的访客
                    UVCt = 1l;
                    // 并把该mid中的状态更新为当前日期时间戳
                    // 不更新的话 第二条数据来了后还是会当作 一个新的日活
                    lastvisitDtState.update(curDt);
                }

                // 定义会话数
                Long SVCt = 0l;
                if (lastPageID == null) {
                    SVCt = 1L;
                }

                //定义浏览总时长
                Long DuringTime = value.getJSONObject("page").getLong("during_time");


                return new TrafficPageViewBean("", "", curVc, curCh, curAr, isNew, UVCt, SVCt, 1l, DuringTime, ts);


            }
        });


        //6.0 提取事件事件生成wartermark
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewBeanwithWMDS = trafficPageViewBean.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner((bean, ts) -> bean.getTs()));

        //7.0 分组开窗聚合
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS = trafficPageViewBeanwithWMDS.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    //流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表

                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {
                        return new Tuple4<>(
                                value.getAr(),
                                value.getIsNew(),
                                value.getCh(),
                                value.getVc()
                        );
                    }
                })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        SingleOutputStreamOperator<TrafficPageViewBean> resultDS = windowDS.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {

                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());

                return value1;

            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {


                // 获取数据
                TrafficPageViewBean next = input.iterator().next();

                // 补充信息
                next.setTs(System.currentTimeMillis()); // 设置版本信息
                next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                next.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                // 输出数据
                out.collect(next);
            }
        });

        //8.0 将数据写出到Clickhouse1
        resultDS.print("result>> : ");
        resultDS.addSink(MyClickhouseUtil.getSinkFunction( "insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?)"));
        //9.0 执行
        env.execute();


        /**                                                                                         ↓
         * ┌─────────────────stt─┬─────────────────edt─┬─vc───────┬─ch────────┬─ar─────┬─is_new─┬─uv_ct─┬─sv_ct─┬─pv_ct─┬─dur_sum─┬────────────ts─┐
         * │ 2022-02-23 23:24:30 │ 2022-02-23 23:24:40 │ v2.1.134 │ Appstore  │ 110000 │ 0      │     4 │     4 │    15 │  163200 │ 1676820472467 │
         *
         */


    }
}
