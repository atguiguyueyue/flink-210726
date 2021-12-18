package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

public class Flink13_TbaleAPI_EventTime_TumblingWindow {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                );

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 3.将流转为表，并指定事件时间字段
//        Table table = tableEnv.fromDataStream(waterSensorStream, $("id"), $("ts"), $("vc"), $("et").rowtime());
        //在已有字段指定为事件时间
//        Table table = tableEnv.fromDataStream(waterSensorStream, $("id"), $("ts").rowtime(), $("vc"));
        Table table = tableEnv.fromDataStream(waterSensorStream, $("id"), $("ts"), $("vc"),$("pt").proctime());


        Table resultTable = table
                //开启一个三秒的滚动窗口
//                .window(Tumble.over(lit(3).seconds()).on($("ts")).as("w"))
                //开启一个滑动窗口，窗口大小是3s，滑动步长为2s
//                .window(Slide.over(lit(3).second()).every(lit(2).second()).on($("ts")).as("w"))
                //开启一个会话窗口，会话间隔为2s
//                .window(Session.withGap(lit(2).seconds()).on($("ts")).as("w"))
                //开启一个计数的滚动窗口,窗口大小为2
                .window(Tumble.over(rowInterval(2L)).on($("pt")).as("w"))
                .groupBy($("id"), $("w"))
//                .select($("id"), $("vc").sum(), $("w").start().as("start"), $("w").end().as("end"));
                .select($("id"), $("vc").sum());


        resultTable.execute().print();
    }
}
