package com.study.flinkDemo.sql;

import com.study.flinkDemo.env.FlinkEnv;
import com.study.flinkDemo.pojo.Info;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * @author feng xl
 * @date 2021/6/24 0024 19:49
 */


/**
 * 初次使用
 * */
public class TableRead {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnv.flinkEnv;
        DataStream<String> source = env.readTextFile("E:\\IntelliJ IDEA 2018.3\\FlinkStudy\\src\\main\\resources\\info.txt");
        DataStream<Info> map = source.map(line -> {
            String[] strings = line.split(",");
            return new Info(new Integer(strings[0]), strings[1], new Integer(strings[2]));
        });
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table infoTable = tableEnv.fromDataStream(map);
        tableEnv.createTemporaryView("info", infoTable);
        String sql = "select * from info";
        Table sqlQuery = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(sqlQuery, Info.class).print();
        env.execute();
    }
}
