package com.atguigu.gmall.realtime.app.func;

import com.atguigu.gmall.realtime.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<words STRING>")) // row为 炸出来的行对象 word 是炸出来的列的名称 string是炸出来的数据类型
public class SplitFunction extends TableFunction<Row> {

    public void eval(String str){ // 固定的切词调用方法 形参为要传入的数据

        List<String> list = null;

        try {
            list = KeywordUtil.splitKeyword(str); // 调用切词方法
            for (String word : list) {
                collect(Row.of(word));  // 循环将切出来的词返回到流中
            }
        } catch (IOException e) {
            collect(Row.of(str));// 切词失败干脆返回整个str ,不切了,整体返回
        }
    }

}


