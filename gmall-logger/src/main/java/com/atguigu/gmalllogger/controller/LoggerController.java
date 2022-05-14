package com.atguigu.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
// 默认返回一个页面对象
//@RestController
@Slf4j
// 返回的是一个普通对象
public class LoggerController {

//    Logger logger = LoggerFactory.getLogger(LoggerController.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("test")
    public String test() {
        System.out.println("222222");
        return "success";
    }

    @RequestMapping("test1")
    public String test1(@RequestParam("name") String name,
                        @RequestParam("age") Integer age) {
        System.out.println("333333");
        return "name:" + name + "-" + "age:" + age;
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String name,
                        // 添加默认值
                        @RequestParam(value = "age", defaultValue = "18") int age) {
        System.out.println("333333");
        System.out.println("name:" + name + "-" + "age:" + age);
        return "success.html";
//        return "name:" + name + "-" + "age:" + age;
    }

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr) {
//        System.out.println(jsonStr);

//        logger.info();
        // 将日志打印到控制台，并落盘
        log.info(jsonStr);

        // 将数据发送到Kafka
        kafkaTemplate.send("ods_base_log", jsonStr);

        return "success";
    }

}




























