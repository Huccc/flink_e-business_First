package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface AsyncJoinFunction<T> {
    String getKey(T input);

    void join(T input, JSONObject dimInfo) throws ParseException;

}
