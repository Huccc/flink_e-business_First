package com.atguigu.gmallpublisher1.controller;

import com.atguigu.gmallpublisher1.service.ProductStatsService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Map;

@RestController
// 全局的默认所有方法上面带个前缀
@RequestMapping("/api/sugar")
public class SugarController {

    // 自动注入
    @Autowired
    private ProductStatsService productStatsService;

    @RequestMapping("/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {

        if (date == 0) {
            date = getToday();
        }

        // 查询clickhouse数据
        BigDecimal gmv = productStatsService.getGmv(date);

        // 封装JSON并返回

        String json = "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": " + gmv + " " +
                "}";
        return json;
    }

    @RequestMapping("/tm")
    public String getGmvByTm(@RequestParam(value = "date", defaultValue = "0") int date,
                             @RequestParam(value = "limit", defaultValue = "5") int limit) {

        if (date == 0) {
            date = getToday();
        }

        // 查询clickhouse获取品牌GMV数据    ::Map[(苹果->341341),(TCL->98707)]
        Map gmvByTm = productStatsService.getGmvByTm(date, limit);

        // 封装JSON数据并返回结果
        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": { " +
                "    \"categories\": [\"" +
                StringUtils.join(gmvByTm.keySet(), "\",\"") +
                "\"], " +
                "    \"series\": [ " +
                "      { " +
                "        \"name\": \"GMV\", " +
                "        \"data\": [" +
                StringUtils.join(gmvByTm.values(), ",") +
                "] " +
                "      } " +
                "    ] " +
                "  } " +
                "}";

    }

    private int getToday() {
        long ts = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

        return Integer.parseInt(sdf.format(ts));
    }
}


















