package com.atguigu.gmall.realtime.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {
    // 构建线程链接池对象
    // 构建懒汉式或者饿汉式 todo 需要重新复习定义
    // 什么是懒汉式 ? --> 用到的时候再去创建 线程不安全
    // 什么是饿汉式 ? --> 提前创建好 ,用到的时候直接使用 线程安全

    // 如何解决懒汉式的线程安全问题?
    // 使用双重校验的方案

    // 声明静态属性信息
    private static ThreadPoolExecutor threadPoolExecutor;
    // 创建构造器方法
    private ThreadPoolUtil(){}
    // 创建静态方法 调用静态属性信息

    public static ThreadPoolExecutor getThreadPoolExecutor(){


        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(
                            4,20,100, TimeUnit.SECONDS,  new  LinkedBlockingDeque<>()
                            // 工作队列满了之后再去创建新的线程
                    );
                }
            }
        }

        return threadPoolExecutor;
    }

}
