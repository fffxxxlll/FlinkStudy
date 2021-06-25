package com.study.flinkDemo.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author feng xl
 * @date 2021/6/22 0022 9:56
 */
public class StreamWordCount {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(3);

        /*   读取本地txt文本 */
//        String path = "E:\\IntelliJ IDEA 2018.3\\FlinkStudy\\src\\main\\resources\\hello.txt";
//        DataStream<String> inputDataStream= env.readTextFile(path);


        /*
        *  读取虚拟机socket文本
        *  */

        /*   读取执行参数, 实际运行环境中的方式 */

//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.get("host");
//        int port = parameterTool.getInt("port");
        DataStream<String> inputDataStream = env.socketTextStream("hadoop03", 20000);
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new MyFlatFunc())
                .keyBy(0).sum(1);

        resultStream.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
