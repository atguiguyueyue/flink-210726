package com.atguigu.day07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

public class Flink02_CEP_Loop {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件中获取数据
        SingleOutputStreamOperator<WaterSensor> inputDS = env.readTextFile("input/sensor.txt")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs() * 1000;
                                    }
                                })
                );

        //TODO 1.定义规则
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .<WaterSensor>begin("start")
                //迭代条件
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                        return "sensor_1".equals(waterSensor.getId());
                    }
                })
                //简单条件
                /*.where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return value.getVc()>30;
                    }
                })
                .or(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                        return value.getTs()>3;
                    }
                })*/
                //循环两次
//                .times(2)
                //循环两次三次或四次
//                .times(2,4)
                //一次或多次
                .oneOrMore()
                //两次及两次以上
//                .timesOrMore(2)
                //停止条件
                .until(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                        return value.getVc()>=40;
                    }
                })
                ;

        //TODO 2.将规则作用于流上
        PatternStream<WaterSensor> patternStream = CEP.pattern(inputDS, pattern);


        //TODO 3.获取符合规则的数据
        patternStream.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                return pattern.toString();
            }
        }).print();

        env.execute();
    }
}
