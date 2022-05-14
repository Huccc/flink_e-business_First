package com.atguigu.gmallpublisher1.service;

import java.math.BigDecimal;
import java.util.Map;

public interface ProductStatsService {
    // 获取某一天的总交易额
    BigDecimal getGmv(int date);

    Map getGmvByTm(int date, int limit);
}
