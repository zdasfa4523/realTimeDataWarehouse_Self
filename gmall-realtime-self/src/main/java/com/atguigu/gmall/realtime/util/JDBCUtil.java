package com.atguigu.gmall.realtime.util;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

public class JDBCUtil {
    /**
     * 主键:id
     * select count(*) from t;              一行一列
     * select * from t where id = '1001';   单行多列
     * select id from t;                    多行单列
     * select * from t;                      多行多列
     */

    public static <T> List<T> queryList(Connection connection ,String querySql,Class<T> clz,boolean underScoreToCamel) throws Exception {
        // 创建集合用于存放结果数据
        ArrayList<T> list = new ArrayList<>();

        // 编译sql
        PreparedStatement pstm = connection.prepareStatement(querySql);

        // 执行查询
        ResultSet resultSet = pstm.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        // 遍历查询结果 ,将每行数据转换为T对象放入集合
        while (resultSet.next()) {
            // 构建T对象
            T t = clz.newInstance();
            // 遍历列
            for (int i = 0; i < columnCount; i++) {
                // 获取列名和数据
                String columnName = metaData.getColumnName(i + 1);
//                resultSet.getObject(i +1 );
                Object value = resultSet.getObject(columnName);

                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());
                }

                BeanUtils.setProperty(t,columnName,value);

            }


            // 将封装好的T对象添加至集合
            list.add(t);
        }
        // 释放资源
        resultSet.close();
        pstm.close();
        // 链接是外部传入的, 在哪里创建 在哪里关闭
  /*      connection.close();*/

        // 返回集合

        return list;
    }

/*    public static void main(String[] args) throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall-config-0828?" +
                "user=root&password=123456&useUnicode=true&" +
                "characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false"
        );

        List<TableProcess> tableProcesses = queryList(connection, "select * from table_process", TableProcess.class, true);

        for (TableProcess tableProcess : tableProcesses) {
            System.out.println(tableProcess);
        }
    }*/
}
