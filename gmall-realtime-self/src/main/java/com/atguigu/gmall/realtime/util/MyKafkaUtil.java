package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyKafkaUtil {

    // 定义一个方法以便消费Kafka中的数据
    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer (String topic ,String groupId){
        Properties conf = new Properties();
        conf.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, GmallConfig.KAFKA_SERVER);
        conf.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);

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
                        }else {
//                            return Arrays.toString(record.value());
                            return new String(record.value());
                        }
                    }
                },
                conf);
    }


}
