package com.study.flinkDemo.stream;


import com.study.flinkDemo.constants.Constants;
import com.study.flinkDemo.env.FlinkEnv;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author feng xl
 * @date 2021/6/24 0024 17:19
 */

/**
 * 读取生产者放入的消息，打印输出
 *
 * */
public class FlinkKafkaConsumer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnv.flinkEnv;
        DataStream<String> source = env.addSource(
                new FlinkKafkaConsumer011<>(Constants.KAFKA_TOPIC,
                        new SimpleStringSchema(), FlinkEnv.properties));
        source.print();
        env.execute();
    }
}
