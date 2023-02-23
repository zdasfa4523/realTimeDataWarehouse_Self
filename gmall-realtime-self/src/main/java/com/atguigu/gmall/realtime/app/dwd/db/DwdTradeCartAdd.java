package com.atguigu.gmall.realtime.app.dwd.db;


import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.atguigu.gmall.realtime.util.MySqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

//交易域加购事实表
/*
1.定义来源：kafka里topic_db(多层级json) 读全量然后过滤出cart_info
           lookup表  base_dic(mysql、hbase)（需要主流添加处理时间）
2.sql 从topic_db中提取加购事实数据
3.用lookupjoin关联base_dic
4.定义目标表：dwd_trade_cart_add
5.写入目标表：insert into dwd_trade_cart_add select xxx from xxx
 */
//todo 3.创建动态表topic_db读取kafka主题topic_db(主流建表的时候要设置处理时间字段（lookup join要求）)
//todo 4.过滤出加购表数据
//todo 5.连接mysql读取base_dic表构建base_dic动态表
//todo 6.关联加购表与base_dic动态表
//todo 7.创建动态表dwd_cart_info连接kafka的dwd_trade_cart_add主题
//todo 8.将关联的后的数据查询出来写到动态表dwd_cart_info里
public class DwdTradeCartAdd {
    public static void main(String[] args) {
        //todo 1环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //todo 2.设置检查点和状态后端
        //        // 需要从Checkpoint或者Savepoint启动程序 需启动hdfs
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

        /*
       kafka里topic_db存储的是 Maxwell数据格式:
{"database":"gmall","table":"cart_info","type":"update","ts":1592270938,"xid":13090,"xoffset":1573,"data":{"id":100924,"user_id":"93","sku_id":16,"cart_price":4488,"sku_num":1,"img_url":"http://47.93.148.192:8080/group1/M0rBHu8l-sklaALrngAAHGDqdpFtU741.jpg","sku_name":"华为 HUAWEI P40 麒麟990 5G SoC芯片 5000万30倍数字变焦 8GB+128GB亮黑色全网通5G手机","is_checked":null,"create_time":"2020-06-14 09:28:57","operate_time":null,"is_ordered":1,"order_time":"2021-10-17 09:28:58","source_type":"2401","source_id":null},"old":{"is_ordered":0,"order_time":null}}

{
    "database":"gmall",
    "table":"cart_info",
    "type":"update",
    "ts":1592270938,
    "xid":13090,
    "xoffset":1573,
    "data":{
        "id":100924,
        "user_id":"93",
        "sku_id":16,
        "cart_price":4488,
        "sku_num":1
    },
    "old":{
        "is_ordered":0,
        "order_time":null
    }
}
         */
        //todo 3.创建动态表topic_db读取kafka主题topic_db(主流建表的时候要设置处理时间字段（lookup join要求）)
//        System.out.println(KafkaUtil.getTopicDbDDL("dwd_cart_add_220828"));
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_cart_add_2208209"));

        //todo 4.从动态表topic_db过滤出加购表数据
        Table cartInfoTable = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['sku_id'] sku_id, " +
                "data['cart_price'] cart_price, " +
                "data['sku_num'] sku_num, " +
                "data['sku_name'] sku_name, " +
                "data['is_checked'] is_checked, " +
                "data['create_time'] create_time, " +
                "data['operate_time'] operate_time, " +
                "data['is_ordered'] is_ordered, " +
                "data['order_time'] order_time, " +
                "data['source_type'] source_type, " +
                "data['source_id'] source_id, " +
                "pt " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='cart_info' " +//从动态表topic_db过滤出加购表数据
                "and `type`='insert' ");
        //注册表名为cart_info，方便后面关联查询
        tableEnv.createTemporaryView("cart_info",cartInfoTable);

        //todo 5.连接mysql读取base_dic表构建base_dic动态表
        tableEnv.executeSql(MySqlUtil.getBaseDicDDL());

        //todo 6.关联加购表与base_dic动态表 (lookup join)
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "c.id,\n" +
                "c.user_id,\n" +
                "c.sku_id,\n" +
                "c.cart_price,\n" +
                "c.sku_num,\n" +
                "c.sku_name,\n" +
                "c.is_checked,\n" +
                "c.create_time,\n" +
                "c.operate_time,\n" +
                "c.is_ordered,\n" +
                "c.order_time,\n" +
                "c.source_type,\n" +
                "c.source_id,\n" +
                "d.dic_name\n" +
                "from cart_info as c \n" +
                "join base_dic for system_time as of c.pt as d \n" +//lookup join
                "on c.source_type=d.dic_code");
//        resultTable.execute().print();
        // 注册表名，方便后面查询
        tableEnv.createTemporaryView("result_table",resultTable);


        //todo 7.创建动态表dwd_cart_info连接kafka的dwd_trade_cart_add主题
        tableEnv.executeSql("create table dwd_cart_info(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "cart_price string,\n" +
                "sku_num string,\n" +
                "sku_name string,\n" +
                "is_checked string,\n" +
                "create_time string,\n" +
                "operate_time string,\n" +
                "is_ordered string,\n" +
                "order_time string,\n" +
                "source_type string,\n" +
                "source_id string,\n" +
                "dic_name string\n" +
                ")"+MyKafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add") );

        //todo 8.将关联的后的数据查询出来写到动态表dwd_cart_info里
        tableEnv.executeSql("insert into dwd_cart_info select * from result_table");


    }
}
