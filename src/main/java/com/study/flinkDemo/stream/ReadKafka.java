package com.study.flinkDemo.stream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author feng xl
 * @date 2021/6/24 0024 16:35
 */

/**
 * 初次使用
 * */
public class ReadKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop01:9092");
        DataStream<String> sensor = env.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));
        sensor.print();
        env.execute();
    }
}
