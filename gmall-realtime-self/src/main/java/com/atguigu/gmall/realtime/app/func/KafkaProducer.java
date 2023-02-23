package com.atguigu.gmall.realtime.app.func;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;


public class KafkaProducer {
    public static void main(String[] args) throws InterruptedException, ParseException {


        // 1. 创建kafka生产者的配置对象
        Properties properties = new Properties();

        // 2. 给kafka配置对象添加配置信息
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        // key,value序列化（必须）：key.serializer，value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 3. 创建kafka生产者对象
        org.apache.kafka.clients.producer.KafkaProducer<String, String> kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);


        String dateTime = "2022-02-21 12:58:29";
        // 4. 调用send方法,发送消息
        for (int i = 0; i < 5000 ; i++) {

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date curDate = simpleDateFormat.parse(dateTime);
            TimeStamp timeStamp = new TimeStamp(curDate);
            long curtimeStamp = timeStamp.getTime();

            curtimeStamp += i;

            String stamp = String.valueOf(curtimeStamp);


            String jsonText = "{\"id\":\"17935\",\"order_id\":\"6602\",\"sku_id\":\"9\",\"sku_name\":\"Apple iPhone 12 (A2404) 64GB 红色 支持移动联通电信5G 双卡双待手机\",\"order_price\":\"8197.0\",\"sku_num\":\"3\",\"create_time\":"+stamp+",\"source_type\":\"2404\",\"source_id\":\"2\",\"split_total_amount\":\"24091.0\",\"split_activity_amount\":\"500.0\",\"split_coupon_amount\":null,\"consignee\":\"葛以建\",\"consignee_tel\":\"13210836651\",\"total_amount\":\"60950.0\",\"order_status\":\"1001\",\"user_id\":\"2130\",\"delivery_address\":\"第9大街第32号楼4单元152门\",\"order_comment\":\"描述887556\",\"out_trade_no\":\"957478498931255\",\"trade_body\":\"金沙河面条 银丝挂面900g*3包 爽滑 细面条 龙须面 速食面等9件商品\",\"process_status\":null,\"tracking_no\":null,\"parent_order_id\":null,\"province_id\":\"19\",\"activity_reduce_amount\":\"500.0\",\"coupon_reduce_amount\":\"0.0\",\"original_total_amount\":\"61433.0\",\"feight_fee\":\"17.0\",\"feight_fee_reduce\":null,\"refundable_time\":null,\"activity_id\":\"2\",\"activity_rule_id\":\"4\",\"coupon_id\":null,\"coupon_use_id\":null,\"dic_name\":\"未支付\"}\n";

            // 添加回调
            kafkaProducer.send(new ProducerRecord<>("dwd_trade_order_detail", jsonText), new Callback() {

                // 该方法在Producer收到ack时调用，为异步调用
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {

                    if (exception == null) {
                        // 没有异常,输出信息到控制台
                        System.out.println("主题：" + metadata.topic() + "->" + "分区：" + metadata.partition());
                    } else {
                        // 出现异常打印
                        exception.printStackTrace();
                    }
                }
            });

            // 延迟一会会看到数据发往不同分区
            Thread.sleep(1000);
        }

        // 5. 关闭资源
        kafkaProducer.close();
    }
}



