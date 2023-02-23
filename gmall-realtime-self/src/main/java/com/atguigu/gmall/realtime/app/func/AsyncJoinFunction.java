package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

public interface AsyncJoinFunction<T> {
//    public  abstract String getKey(T input) ;
    String getKey(T input) ;
    // 通过传入的input得到相应的key
    // 但是要得到相关的具体的字段信息
    // 直接提升为抽象方法

//    public abstract void join(T input, JSONObject dimInfo) ;
    void join(T input, JSONObject dimInfo) ;
    //input 是传入的javabean信息 dimInfo是传入的维度信息
}
