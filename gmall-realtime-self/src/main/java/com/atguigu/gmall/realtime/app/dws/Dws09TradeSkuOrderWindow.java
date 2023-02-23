package com.atguigu.gmall.realtime.app.dws;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;


// 数据流:web -> mysql ->Maxwell -> Kf(ODS) -> FlinkApp -> KF(DWD) -> FlinkAPP ->  clickhouse
// 数据流:web -> mysql ->Maxwell -> kf(ODS) -> FlinkAPP -> kf(主流)
                                                                // ===> Phoenix(DIM)
// 配置流: Mysql -> FlinkCDC(配置流)

// 程序:  Mock -> Mysql -> Maxwell -> Kf(zk) -> DwdTradeOrderDetail -> Kf(zk)

public class Dws09TradeSkuOrderWindow {
    public static void main(String[] args) throws Exception {

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取Kafka订单明细主题数据创建流
        String topic = "dwd_trade_order_detail";
        String groupId = "Dws09TradeSkuOrderWindow_003";

        DataStreamSource<String> kfSource = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // 3. 过滤null值并转换为Json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kfSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                if ((value != null) && (value.length() >0)) {

                    System.out.println("value =>>> " + value);

                    JSONObject jsonObject = JSONObject.parseObject(value);
                    // 别忘了往外输出
                    System.out.println("jsonObject >>  " + jsonObject);
                    out.collect(jsonObject);
                }
            }
        });

        // 4. 提取时间戳并生成watermark
        SingleOutputStreamOperator<JSONObject> watermarkDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {

/*                String createTime = element.getString("create_time");

//                return DateFormatUtil.toTs(createTime, true);

                System.out.println("createTime => " + createTime);

                System.out.println("element.getLong(createTime) = " + element.getLong(createTime));  // 這個地方相當於拿到的k是 2022-02-21 12:48:28 拿著這個日期格式的key去json中去找對應的value值

//                return element.getLong(createTime);

                System.out.println("return 中的時間錯 : "+ element.getLong("create_time"));*/

                System.out.println("element.getLong(\"create_time\") = >>>>> " + element.getLong("create_time"));


                return element.getLong("create_time");
            }
        }));


        // 指定id 來分組 因此每組就是訂單詳情id

        // 這張表讀取的是dwd_trade_order_detail ,對於這張表來説 我們只要去除符合javaBean中需要的字段即可 也就是相當於
        // dwd_trade_order_detail 中無所謂哪一條數據來 ,因爲這張表是通過 order_detail表關聯得到的,因此我們只要的是隨便一條即可
        // 對於 order_detail 表來説 這張表是主鍵表 因此必定有主鍵相關的字段 因此對於我們來説 無關時間順序 ,只要是隨便的一條
        // 必定有主鍵相關的字段 因此 只需要任意一條即可 對於狀態來説 無所謂存儲什麽,因爲我們只要一條存儲的標記即可

        // 5. 按照唯一鍵(訂單明細id)分組去重
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.keyBy(json -> json.getString("id")).filter(new RichFilterFunction<JSONObject>() {


            // 狀態的聲明
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {

                // 這個state要不要清除?
                // 要不要清除 取決於 這個訂單id未來的話還會出現嗎?
                // 答案: 明顯不會再次出現 這幾條join關聯數據過了時間之後沒有作用 了
                // 因此 ttl的過期時間應該設置為多久
                // 主表中的數據幾乎是同時生成的 因此只需要滿足最小的亂序程度即可 因此設置為亂序程度即可

                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5)).setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite).build();


                // 狀態的定義
                ValueStateDescriptor<String> mark = new ValueStateDescriptor<String>("mark", String.class);

                mark.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(mark);

            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                String mark = valueState.value();

                System.out.println("valueState.value() = " + valueState.value());

                // 根據mark的是否為null進行判斷
                if (mark == null) {
                    // 説明之前沒有數據 ,這個是第一條的數據
                    valueState.update("1");
                    // 將這條數據返回到流中説明就是一條主表中的數據而已
                    System.out.println("狀態中沒有數據 .... ");
                    return true;
                } else {
                    // 説明之前狀態中存儲了數據 因此不是第一條的數據 這條數據應該被丟棄
                    System.out.println(" 狀態中有數據  .... ");
                    return false;
                }
            }
        });
        // 6. 将数据转换为JavaBean

        SingleOutputStreamOperator<TradeSkuOrderBean> tradeSkuOrderDS = filterDS.map(line -> {

            // 暫時封裝 TradeSkuOrderBean 因爲這個javaBean的源頭是 dwd_trade_order_detail數據
            // dwd_trade_order_detail的數據中存在數據,因此能夠進行部分的數據封裝

            BigDecimal splitActivityAmount = line.getBigDecimal("split_activity_amount");
            // 判斷這個活動金額是否為null
            if (splitActivityAmount == null) {
                // 如果為null的話就給null附上值
                splitActivityAmount = new BigDecimal("0.0");
            }

            BigDecimal splitCouponAmount = line.getBigDecimal("split_coupon_amount");
            if (splitCouponAmount == null) {
                splitCouponAmount = new BigDecimal("0.0");
            }



            BigDecimal originalAmount = line.getBigDecimal("originalAmount");
            if (originalAmount == null) {
                originalAmount = new BigDecimal("0.0");
            }


            // 現在拿到的是相當於主表中的數據 先將主表中有的字段進行數據填充
            return TradeSkuOrderBean.builder()
                    .skuId(line.getString("sku_id"))
                    .skuName(line.getString("sku_name"))
                    .originalAmount(originalAmount)
                    .activityAmount(splitActivityAmount)
                    .couponAmount(splitCouponAmount)
                    .orderAmount(line.getBigDecimal("split_total_amount"))
                    .build();

        });




        // 7. 分組開窗聚合
        // 爲什麽能夠與直接與sku_id進行keyby 原因:sku_id就是最細粒度 因此能夠得到最細維度的數據
        // SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS =


        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = tradeSkuOrderDS.keyBy(TradeSkuOrderBean::getSkuId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))) // 開個10秒大小的事件窗口
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setActivityAmount(value1.getActivityAmount().add(value2.getActivityAmount()));
                        value1.setCouponAmount(value1.getCouponAmount().add(value2.getCouponAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));

                        System.out.println("value1.toString() =>>>>> " + value1.toString());

                        return value1;
                    }
                }, new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeSkuOrderBean> input, Collector<TradeSkuOrderBean> out) throws Exception {

                        System.out.println("input =>>>>>> " + input);

                        TradeSkuOrderBean next = input.iterator().next();

                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                        System.out.println("next.toString() = >>>>>>>   " + next.toString());

                        out.collect(next);
                    }
                });

        // 打印測試
//        reduceDS.print("reduceDS>>>>>  ");

        // 關聯維表
/*        reduceDS.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
            private DruidDataSource druidDataSource;

            @Override
            public void open(Configuration parameters) throws Exception {
                druidDataSource = DruidDSUtil.getDruidDataSource();
            }

            @Override
            public TradeSkuOrderBean map(TradeSkuOrderBean value) throws Exception {
                // 補充之前在filter之後的維度字段信息
                // 首先關聯sku_info表  因爲只有sku_info表中有spu_id,tm_id,category3_id 信息 因此首先要關聯拿到spu_id信息
                DruidPooledConnection connection = druidDataSource.getConnection();
                // 關聯sku_info
                // 思考反省字段要填寫什麽?
                DimHandler.getDimInfo(connection,"dim_sku_info",value.getSkuId());
                // 關聯spu
                JDBCUtil.queryList(connection,"select * from dim_spu_info where id = '" +value.getSpuId() +"'", JSONObject.class, false);
                // 關聯tradeMark
                // 關聯Category3
                // 關聯Category2
                // 關聯Category1
                connection.close();
                return value;

            }
        });*/


        // 使用异步线程的方式去跟维表关联
        // 每当调用该方法时传入需要关联的表的名称
        // 关联sku_info表
 /*       SingleOutputStreamOperator<TradeSkuOrderBean> reduceWithSkuDS = AsyncDataStream
                .unorderedWait(reduceDS, new AsyncDimFunction<TradeSkuOrderBean>("DIM_SKU_INFO") {


                    @Override
                    public String getKey(TradeSkuOrderBean input) {
                        // 在外部实现抽象方法 得到需要的key
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dimInfo) {
                        // 实现对javaBean的字段补充 --> 补充sku_id字段/TM_ID/CATEGORY3_ID 字段信息
                        input.setSpuId(dimInfo.getString("SPU_ID"));
                        input.setTrademarkId(dimInfo.getString("TM_ID"));
                        input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                    }
                }, 1, TimeUnit.HOURS);

        reduceWithSkuDS.print("关联sku_info(补充spu_id, trademark_id , category3_id)>>>>>>");

        // reduceWithSkuDS 补充完毕之后就有了spu_id / trademark_id / category3_id

        // 关联spu_info表
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceWithSpuDS = AsyncDataStream
                .unorderedWait(reduceWithSkuDS, new AsyncDimFunction<TradeSkuOrderBean>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(TradeSkuOrderBean input) {
                        // 在外部实现抽象方法 得到需要的key信息
                        return input.getSpuId();
                    }

                    @Override
                    public void join(TradeSkuOrderBean input, JSONObject dimInfo) {
                        // 实现对javaBean的字段补充 --> 补充spuName
                        input.setSpuName(dimInfo.getString("SPU_NAME"));
                    }
                }, 1, TimeUnit.HOURS);

        // reduceWithSpuDS 补充完毕之后有spuname
        reduceWithSpuDS.print("关联SPU_info,补充SPU_NAME >>>>>");
*/



        env.execute("Dws09TradeSkuOrderWindow");


    }
}
