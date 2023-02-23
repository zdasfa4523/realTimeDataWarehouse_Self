package com.atguigu.gmall.realtime.common;

public class GmallConfig {
    // Kafka 服务器地址:
    public static final String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    // Phoenix 的库名
    public static final String PHOENIX_DB = "GMALL_REALTIME_220828";
    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";


    // ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/gmall_0828";



}
