package com.atguigu.gmall.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {

    public static List<String> splitKeyword(String keyword) throws IOException {
        // 创建集合用于存放切分后的数据
        ArrayList<String> list = new ArrayList<>();

        // 创建IK分词对象 ik_smart ik_max_word
/*        *
         * 形参内部:StringReader需要的切词对象*/

        StringReader reader = new StringReader(keyword);
        //1.0 创建IK分词对象  参数需要 一个StringReader对象
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        //循环取出切分好的词
        Lexeme next = ikSegmenter.next(); // 相当于一个数组结果 ,需要遍历取出里面的结果
        while (next != null) { // 利用循环取出切除的词语
            String word = next.getLexemeText(); //取出切分的单词
            list.add(word);
            // 取出来之后指向下一个 ,如果有值的话会继续进入循环 ,如果没有值的话,下次的循环条件进入不进去
            next = ikSegmenter.next();
        }
        // 最终返回集合
        return list;
    }




}


