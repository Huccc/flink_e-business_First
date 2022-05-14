package com.atguigu.gmallpublisher1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 调用流程:
 * web界面请求地址 -> controller拦截 -> service -> service.impl.ProductStatsSericeImpl.. -> mapper -> ProductStatsMapper -> ClinkHouse
 *
 */
@SpringBootApplication
//@MapperScan(basePackages = "com.atguigu.gmallpublisher1.mapper")
public class GmallPublisher1Application {

    public static void main(String[] args) {
        SpringApplication.run(GmallPublisher1Application.class, args);
    }

}
