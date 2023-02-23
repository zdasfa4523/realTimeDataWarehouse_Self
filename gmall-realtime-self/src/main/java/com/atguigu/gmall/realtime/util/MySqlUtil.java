package com.atguigu.gmall.realtime.util;

public class MySqlUtil {

    public static String getBaseDicDDL() {
        return "create table base_dic( " +
                "    dic_code string, " +
                "    dic_name string, " +
                "    parent_code string, " +
                "    create_time string, " +
                "    operate_time string " +
                ")" + getMysqlConn("base_dic");
    }

    public static String getMysqlConn(String tableName) {
        return " WITH ( " +
                "  'connector' = 'jdbc', " +
                "  'url' = 'jdbc:mysql://hadoop102:3306/gmall', " +
                "  'table-name' = '" + tableName + "', " +
                "  'lookup.cache.max-rows' = '10', " +
                "  'lookup.cache.ttl' = '1 hour', " +
                "  'username' = 'root', " +
                "  'password' = '123456', " +
                "  'driver' = 'com.mysql.cj.jdbc.Driver' " +
                ")";
    }

}
