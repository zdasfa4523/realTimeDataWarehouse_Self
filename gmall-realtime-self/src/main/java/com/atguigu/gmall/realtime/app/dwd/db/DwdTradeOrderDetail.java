package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.atguigu.gmall.realtime.util.MySqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderDetail {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.使用FlinkSQL DDL的方式读取Kafka topic_db 主题数据创建动态表
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_order_detail_220828"));

        //TODO 3.过滤订单明细数据
        Table orderDetailTable = tableEnv.sqlQuery("" +
                "select " +
                "    `data`['id'] id,  " +
                "    `data`['order_id'] order_id,  " +
                "    `data`['sku_id'] sku_id,  " +
                "    `data`['sku_name'] sku_name,  " +
                "    `data`['order_price'] order_price,  " +
                "    `data`['sku_num'] sku_num,  " +
                "    `data`['create_time'] create_time,  " +
                "    `data`['source_type'] source_type,  " +
                "    `data`['source_id'] source_id,  " +
                "    `data`['split_total_amount'] split_total_amount,  " +
                "    `data`['split_activity_amount'] split_activity_amount,  " +
                "    `data`['split_coupon_amount'] split_coupon_amount, " +
                "    `pt` " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_detail' " +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail", orderDetailTable);

        //打印测试
        //tableEnv.sqlQuery("select * from order_detail").execute().print();

        //TODO 4.过滤订单数据
        Table orderInfoTable = tableEnv.sqlQuery("" +
                "select " +
                "    `data`['id'] id, " +
                "    `data`['consignee'] consignee, " +
                "    `data`['consignee_tel'] consignee_tel, " +
                "    `data`['total_amount'] total_amount, " +
                "    `data`['order_status'] order_status, " +
                "    `data`['user_id'] user_id, " +
                "    `data`['payment_way'] payment_way, " +
                "    `data`['delivery_address'] delivery_address, " +
                "    `data`['order_comment'] order_comment, " +
                "    `data`['out_trade_no'] out_trade_no, " +
                "    `data`['trade_body'] trade_body, " +
                "    `data`['create_time'] create_time, " +
                "    `data`['operate_time'] operate_time, " +
                "    `data`['expire_time'] expire_time, " +
                "    `data`['process_status'] process_status, " +
                "    `data`['tracking_no'] tracking_no, " +
                "    `data`['parent_order_id'] parent_order_id, " +
                "    `data`['province_id'] province_id, " +
                "    `data`['activity_reduce_amount'] activity_reduce_amount, " +
                "    `data`['coupon_reduce_amount'] coupon_reduce_amount, " +
                "    `data`['original_total_amount'] original_total_amount, " +
                "    `data`['feight_fee'] feight_fee, " +
                "    `data`['feight_fee_reduce'] feight_fee_reduce, " +
                "    `data`['refundable_time'] refundable_time " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_info' " +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_info", orderInfoTable);

        //打印测试
        //tableEnv.sqlQuery("select * from order_info").execute().print();

        //TODO 5.过滤订单明细活动表数据
        Table orderActivityTable = tableEnv.sqlQuery("" +
                "select " +
                "    `data`['id'] id, " +
                "    `data`['order_id'] order_id, " +
                "    `data`['order_detail_id'] order_detail_id, " +
                "    `data`['activity_id'] activity_id, " +
                "    `data`['activity_rule_id'] activity_rule_id, " +
                "    `data`['sku_id'] sku_id, " +
                "    `data`['create_time'] create_time " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_detail_activity' " +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_activity", orderActivityTable);

        //打印测试
        //tableEnv.sqlQuery("select * from order_activity").execute().print();

        //TODO 6.过滤订单明细优惠券表数据
        Table orderCouponTable = tableEnv.sqlQuery("" +
                "select " +
                "    `data`['id'] id,  " +
                "    `data`['order_id'] order_id,  " +
                "    `data`['order_detail_id'] order_detail_id,  " +
                "    `data`['coupon_id'] coupon_id,  " +
                "    `data`['coupon_use_id'] coupon_use_id,  " +
                "    `data`['sku_id'] sku_id,  " +
                "    `data`['create_time'] create_time " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_detail_coupon' " +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_coupon", orderCouponTable);

        //打印测试
        //tableEnv.sqlQuery("select * from order_coupon").execute().print();

        //TODO 7.构建base_dic表
        tableEnv.executeSql(MySqlUtil.getBaseDicDDL());

        //TODO 8.五表关联
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    od.id, " +
                "    od.order_id, " +
                "    od.sku_id, " +
                "    od.sku_name, " +
                "    od.order_price, " +
                "    od.sku_num, " +
                "    od.create_time, " +
                "    od.source_type, " +
                "    od.source_id, " +
                "    od.split_total_amount, " +
                "    od.split_activity_amount, " +
                "    od.split_coupon_amount, " +
                "    oi.consignee, " +
                "    oi.consignee_tel, " +
                "    oi.total_amount, " +
                "    oi.order_status, " +
                "    oi.user_id, " +
                "    oi.delivery_address, " +
                "    oi.order_comment, " +
                "    oi.out_trade_no, " +
                "    oi.trade_body, " +
                "    oi.process_status, " +
                "    oi.tracking_no, " +
                "    oi.parent_order_id, " +
                "    oi.province_id, " +
                "    oi.activity_reduce_amount, " +
                "    oi.coupon_reduce_amount, " +
                "    oi.original_total_amount, " +
                "    oi.feight_fee, " +
                "    oi.feight_fee_reduce, " +
                "    oi.refundable_time, " +
                "    oa.activity_id, " +
                "    oa.activity_rule_id, " +
                "    oc.coupon_id, " +
                "    oc.coupon_use_id, " +
                "    dic.dic_name " +
                "from order_detail od " +
                "join order_info oi " +
                "on od.order_id=oi.id " +
                "left join order_activity oa " +
                "on od.id=oa.order_detail_id " +
                "left join order_coupon oc " +
                "on od.id=oc.order_detail_id " +
                "join base_dic FOR SYSTEM_TIME AS OF od.pt AS dic " +
                "on oi.order_status=dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        //TODO 9.构建Kafka下单表
        tableEnv.executeSql("" +
                "create table dwd_order_detail( " +
                "    id string, " +
                "    order_id string, " +
                "    sku_id string, " +
                "    sku_name string, " +
                "    order_price string, " +
                "    sku_num string, " +
                "    create_time string, " +
                "    source_type string, " +
                "    source_id string, " +
                "    split_total_amount string, " +
                "    split_activity_amount string, " +
                "    split_coupon_amount string, " +
                "    consignee string, " +
                "    consignee_tel string, " +
                "    total_amount string, " +
                "    order_status string, " +
                "    user_id string, " +
                "    delivery_address string, " +
                "    order_comment string, " +
                "    out_trade_no string, " +
                "    trade_body string, " +
                "    process_status string, " +
                "    tracking_no string, " +
                "    parent_order_id string, " +
                "    province_id string, " +
                "    activity_reduce_amount string, " +
                "    coupon_reduce_amount string, " +
                "    original_total_amount string, " +
                "    feight_fee string, " +
                "    feight_fee_reduce string, " +
                "    refundable_time string, " +
                "    activity_id string, " +
                "    activity_rule_id string, " +
                "    coupon_id string, " +
                "    coupon_use_id string, " +
                "    dic_name string, " +
                "    PRIMARY KEY (id) NOT ENFORCED " +
                ")" + MyKafkaUtil.getKafkaUpsertSinkDDL("dwd_trade_order_detail"));

        //TODO 10.将数据写出
        tableEnv.executeSql("insert into dwd_order_detail select * from result_table");

    }
}
