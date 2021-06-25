package com.study.flinkDemo.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author feng xl
 * @date 2021/6/21 0021 21:19
 */
public class WordCount {

    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //请使用绝对路径
        String path = "E:\\IntelliJ IDEA 2018.3\\FlinkStudy\\src\\main\\resources\\hello.txt";
        DataSet<String> stringDataSet= env.readTextFile(path);

        /*
        * 使用lambda表达式出现返回类型缺失，提示使用内部类代替;
        * 或者使用returns()声明函数返回类型
        * 也可以使函数实现ResultTypeQueryable接口.
        * */
//        AggregateOperator<Object> aggregateOperator = stringDataSet.flatMap((s, collector) -> {
//            String[] words = s.split(" ");
//            for (String w :
//                    words) {
//                collector.collect(new Tuple2<>(w, 1));
//            }
//        }).groupBy(0).sum(1);

        /*
        * 使用匿名内部类的方式
        * */
//        AggregateOperator<Tuple2<String, Integer>> aggregateOperator = stringDataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                String[] words = value.split(" ");
//                for (String w :
//                        words) {
//                    out.collect(new Tuple2<>(w, 1));
//                }
//            }
//        }).groupBy(0).sum(1);

        /*内部类*/
        AggregateOperator<Tuple2<String, Integer>> aggregateOperator = stringDataSet.flatMap(new MyFlatFunc())
                .groupBy(0)
                .sum(1);
        try {
            aggregateOperator.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

/*
* 使用内部类方式,不实现ResultTypeQueryable接口,没有出现异常
* */
class MyFlatFunc implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector out) throws Exception {
        String[] words = value.split(" ");
        for (String w:
             words) {
            out.collect(new Tuple2<>(w, 1));
        }
    }

}

