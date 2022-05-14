package com.atguigu.utils;

import lombok.SneakyThrows;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPoolExecutor = null;

    public ThreadPoolUtil(ThreadPoolExecutor threadPoolExecutor) {
    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {

        /**
         * 懒汉式  需要加双重校验的同步锁
         * @return
         */
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(
                            4,
                            20,
                            100,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>()
                    );
                }
            }
        }


        return threadPoolExecutor;
    }

    public static void main(String[] args) {
        ThreadPoolExecutor threadPoolExecutor = getThreadPoolExecutor();
        for (int i = 0; i < 10; i++) {
            int j = i;

//            synchronized (ThreadPoolUtil.class) {
                threadPoolExecutor.execute(new Runnable() {
                    @SneakyThrows
                    @Override
                    public void run() {
//                    System.out.println(Thread.currentThread().getName() + i);
                        System.out.println(Thread.currentThread().getName() + ">>>>>" + j);
                        Thread.sleep(10000);
                    }
                });
//            }


        }
    }

}


























