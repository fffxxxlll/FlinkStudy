package com.study.flinkDemo.stream;

import com.study.flinkDemo.constants.Constants;
import com.study.flinkDemo.env.FlinkEnv;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @author feng xl
 * @date 2021/6/24 0024 17:20
 */

/**
 * 读取hadoop3的套接字信息，然后存放到hadoop01的kafka队列
 *
 * */
public class FlinkKafkaProducer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnv.flinkEnv;
        DataStreamSource<String> source = env.socketTextStream(Constants.SOCKET_HOST, Constants.SOCKET_PORT);
        source.addSink(new FlinkKafkaProducer011<>(Constants.KAFKA_TOPIC,
                new SimpleStringSchema(), FlinkEnv.properties));
        env.execute();
    }
}
