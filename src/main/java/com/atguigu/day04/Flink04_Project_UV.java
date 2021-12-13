package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Flink04_Project_UV {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<Tuple2<String, Long>> uvToUserIdDS = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(",");
                UserBehavior userBehavior = new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4]));

                //过滤出pv的数据
                if ("pv".equals(userBehavior.getBehavior())) {
                    out.collect(Tuple2.of("uv", userBehavior.getUserId()));
                }
            }
        });
        
        //4.将相同key的数据聚和到一块
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = uvToUserIdDS.keyBy(0);
        
        //5.对userId做去重求出UV个数
        keyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Long>, Tuple2<String, Integer>>() {

            //创建set集合用来去重
            HashSet<Tuple2<String, Long>> set = new HashSet<>();

            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                //将数据存入Set集合
                set.add(value);
                //取出set集合元素个数就是uv的个数
                int count = set.size();
                out.collect(Tuple2.of("uv", count));
            }
        }).print();


        env.execute();

    }
}
