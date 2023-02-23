package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.bean.KeywordBean;
import com.atguigu.gmall.realtime.util.MyClickhouseUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.atguigu.gmall.realtime.app.func.SplitFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        //todo 1.获取执行环境
// 1.0 获取流的执行环境
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


        // 搜哟关键字页面json格式
/*
        {
            "common": {
            "ar": "440000",
                    "ba": "iPhone",
                    "ch": "Appstore",
                    "is_new": "1",
                    "md": "iPhone Xs Max",
                    "mid": "mid_51315",
                    "os": "iOS 13.2.3",
                    "uid": "603",
                    "vc": "v2.1.132"
        },
            "page": {
            "during_time": 11092,
                    "item": "苹果手机",
                    "item_type": "keyword",
                    "last_page_id": "search",
                    "page_id": "good_list"
        },
            "ts": 1651303985000
        }*/

        //todo 2. 使用DDL方式读取Kafka page_log 主题的数据创建表并且提取时间戳生成watermark
        //因为我们要的是关键字 ,跟common字段无关 ,因此可以不要 ts 需要指定作为开窗的事件时间的时间戳
        TableResult page_log = tableEnv.executeSql("" +
                "create table  page_log ( " + // 创建kf的临时表格
                "   `page` MAP<String,String>, " + // `page` 相当于每一个表的表头 相当于给这列起的名字  在这列中存储的数据类型为map类型
                "   `ts` bigint ," +
                "    `et` AS TO_TIMESTAMP_LTZ(ts, 3) ," + // 利用to_timestamp_ltz 将ts 转换为timestamp3类型
                "    WATERMARK FOR et AS et - INTERVAL '5' SECOND )" + // 列修饰符为`  数字修饰符为 '
                MyKafkaUtil.getKafkaDDL("dwd_traffic_page_log", "DwsTrafficKeywordPageViewWindow_220828"));//注意时间戳的类型为bigint 并非直接就是ts
// watermark for [指定事件时间字段] as [这一列的别名] [指定乱序时间]
        // 实际上在上面的建表的列信息中 只有 `page` `ts` `et` 列会被打印 , WATERMARK FOR et AS et - INTERVAL `5` SECOND 这一列不会被打印 因为是指定watermark使用的

        // todo 3. 过滤出搜索数据
        // 过滤出搜索条件的数据 1. last_page_id :search 2. Item_type 为 keyword类型 3. item关键字不为null
        Table filterTable = tableEnv.sqlQuery("" +
                "select " +
                "   page['item'] item_word , " + //只需要 item 的值信息
                "   et " + // 时间时间字段用于开窗计算
                "from " +
                "   page_log " + //
                "where page['last_page_id'] = 'search' " +
                "   and page['item_type'] = 'keyword' " +
                "   and page['item'] is not null ");
        // 只有sqlQuery才是返回的table 对象,可以进行表的注册 ,而executeSql返回的是TableResult对象 不可以进行表的注册
//        filterTable.execute().print();

        tableEnv.createTemporaryView("filter_table",filterTable);

        // todo 4. 注册UDTF 切词
        tableEnv.createTemporaryFunction("splitfunc", SplitFunction.class);
        Table splitTable = tableEnv.sqlQuery("" +
                "select " +
                "  words," + // 炸裂出来之后的列名信息 在注册的方法中调用
                "  et " + // 事件事件
                "  from " +
                "  filter_table ," + // 注册的表的名称
                "  LATERAL TABLE(splitfunc(item_word)) ");//日用注册的函数填写item_word 是来自filter_table中的列的名称信息

        tableEnv.createTemporaryView("split_table",splitTable);


        // todo 5. 分组 开窗 聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                " DATe_FORMAT(TUMBLE_START(et,Interval '10' second),'yyyy-MM-dd HH:mm:ss') stt , " + //窗口开始事件
                " DATe_FORMAT(TUMBLE_END(et,Interval '10' second),'yyyy-MM-dd HH:mm:ss') edt , " + // 窗口关闭时间
                " words , " + // 炸裂出来的数据
                " count(1) ct ," + //一个窗口内进行聚合
                " unix_timestamp() * 1000 ts " + // 往ck里面存放的时候指定的版本值信息
                " from split_table " + // 来源表
                " group by words ,TUMBLE(et,Interval '10' second)");//按照每个窗口内部求炸裂出来的字符 , 开窗大小

        // todo 只有一条数据不会触发窗口的关闭 如何解决?

        // todo 6. 将动态表转换为流
        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);
        keywordBeanDataStream.print(">>>>>");

        // todo 7. 将数据写出到ClickHouse
        keywordBeanDataStream.addSink(MyClickhouseUtil.getSinkFunction("" +
                "insert into  dws_traffic_keyword_page_view_window values(?,?,?,?,?) "));

        // todo 8. 启动任务
        env.execute("pageviewWindow");
    }
}
