package com.study.flinkDemo.sql;

import com.study.flinkDemo.constants.Constants;
import com.study.flinkDemo.env.FlinkEnv;
import com.study.flinkDemo.pojo.Info;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author feng xl
 * @date 2021/6/24 0024 21:07
 */

/**
 * 使用table api读取kafka
 * */
public class FlinkTableConsumer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnv.flinkEnv;
        StreamTableEnvironment tableEnv = FlinkEnv.tableEnv;
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer011<>(Constants.KAFKA_TOPIC, new SimpleStringSchema(), FlinkEnv.properties));
        SingleOutputStreamOperator<Info> map = source.map(line -> {
            String[] strings = line.split(" ");
            return new Info(new Integer(strings[0]), strings[1], new Integer(strings[2]));
        });
        Table infoTable = tableEnv.fromDataStream(map);
        tableEnv.createTemporaryView("info", infoTable);
        Table sqlQuery = tableEnv.sqlQuery("select * from info");
        DataStream<Info> infoDataStream = tableEnv.toAppendStream(sqlQuery, Info.class);
        infoDataStream.print();
        env.execute();
    }
}
