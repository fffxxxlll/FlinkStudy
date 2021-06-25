package com.study.flinkDemo.env;

import com.study.flinkDemo.constants.Constants;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


import java.util.Properties;

/**
 * @author feng xl
 * @date 2021/6/24 0024 17:21
 */

/**
 * 初始化环境
 * */
public class FlinkEnv {

    public static StreamExecutionEnvironment flinkEnv;
    public static StreamTableEnvironment tableEnv;
    public static Properties properties;
    static {
        flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        flinkEnv.setParallelism(12);
        tableEnv = StreamTableEnvironment.create(flinkEnv);
        properties = new Properties();
        properties.put("bootstrap.servers", Constants.KAFKA_HOST_PORT);
    }
}
