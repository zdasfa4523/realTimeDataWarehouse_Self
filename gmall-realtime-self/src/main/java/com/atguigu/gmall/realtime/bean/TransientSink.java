package com.atguigu.gmall.realtime.bean;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD) // 作用字段
@Retention(RetentionPolicy.RUNTIME) // 运行时机
public @interface TransientSink {

}
