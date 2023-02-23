package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;


public class MyKafkaUtil {

    // 定义一个方法以便消费Kafka中的数据
    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic, String groupId) {
        Properties conf = new Properties();
        conf.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, GmallConfig.KAFKA_SERVER);
        conf.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new FlinkKafkaConsumer<String>(topic,
                //  new SimpleStringSchema(), // 读取的参数不能为null 否则会报空指针
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false; // 无界流永远返回false 因为不是最后一个
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        if (record == null || record.value() == null) {
                            return ""; // return null; 是什么意思? todo Null 和 null 之间有什么区别?
                        } else {
//                            return Arrays.toString(record.value());
                            return new String(record.value());
                        }
                    }
                },
                conf);
    }

    //定义一个方法以便往Kafka中的数据
    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, GmallConfig.KAFKA_SERVER);

        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }

    public static FlinkKafkaProducer<JSONObject> getFlinkKafkaProducer() {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, GmallConfig.KAFKA_SERVER);
        return new FlinkKafkaProducer<JSONObject>("", new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                /*"data":{
                            "id":100924,
                            "user_id":"93",
                            "sku_id":16,
                            "cart_price":4488,
                            "sku_num":1
                        *** "sink_table":"????"
                },*/
                String topic = element.getString("sink_table");
                element.remove("sink_table");
                System.out.println("目标topic分区:" + topic + "数据为:" + element.toJSONString().getBytes(StandardCharsets.UTF_8));
                return new ProducerRecord<>(topic, element.toJSONString().getBytes(StandardCharsets.UTF_8));

            }
        }, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    public static <T> FlinkKafkaProducer<T> getFlinkKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, GmallConfig.KAFKA_SERVER);
        return new FlinkKafkaProducer<T>("", kafkaSerializationSchema, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }


    /**
     * Kafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 Kafka-Sink DDL 语句
     */
    public static String getKafkaSinkDDL(String topic) {
        return " WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + GmallConfig.KAFKA_SERVER + "', " +
                "  'format' = 'json' " +
                ")";
    }

    /**
     * Kafka-Source DDL 语句
     *
     * @param topic   数据源主题
     * @param groupId 消费者组
     * @return 拼接好的 Kafka 数据源 DDL 语句
     */
    public static String getKafkaDDL(String topic, String groupId) {
        return "  with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + GmallConfig.KAFKA_SERVER + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset')";
    }


    public static String getKafkaUpsertSinkDDL(String topic) {
        return " WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' ='" + GmallConfig.KAFKA_SERVER + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }

    public static String getTopicDbDDL(String groupId) {
        return "create table topic_db( " +
                "    `database` STRING, " +
                "    `table` STRING, " +
                "    `type` STRING, " +
                "    `data` MAP<STRING,STRING>, " +
                "    `old` MAP<STRING,STRING>, " +
                "    `pt` AS PROCTIME() " +
                ")" + getKafkaDDL("topic_db", groupId);
    }


}
