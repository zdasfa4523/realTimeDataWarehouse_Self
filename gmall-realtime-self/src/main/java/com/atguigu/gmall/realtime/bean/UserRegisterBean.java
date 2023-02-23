package com.atguigu.gmall.realtime.bean;


import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor

public class UserRegisterBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 用户当前窗口注册人数
    Long UserRegCt;
    // 时间戳
    Long ts;
}
