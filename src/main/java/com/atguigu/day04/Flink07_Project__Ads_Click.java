package com.atguigu.day04;

import com.atguigu.bean.AdsClickLog;
import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Flink07_Project__Ads_Click {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件中获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/AdClickLog.csv");

        //3.将数据转为JavaBean并组成Tuple元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = streamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                AdsClickLog adsClickLog = new AdsClickLog(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        split[2],
                        split[3],
                        Long.parseLong(split[4])
                );

                return Tuple2.of(adsClickLog.getProvince() + "-" + adsClickLog.getAdId(), 1);
            }
        });

        //4.聚和
        map.keyBy(0)
                .sum(1)
                .print();

        env.execute();
    }

}
