package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

public class Flink03_KeyedState_ReducingState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //4.对相同key的数据做聚合
        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(r -> r.getId());

        //5.计算每个传感器水位和
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, Tuple2<String,Integer>>() {
            //1.创建一个ReducingState用来计算水位和并保存
            private ReducingState<Integer> reducingState;

            //初始化状态
            @Override
            public void open(Configuration parameters) throws Exception {
                reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("reducing-state", new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        return value1 + value2;
                    }
                }, Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                //1.先将数据存入状态中做累加计算
                reducingState.add(value.getVc());

                //2.取出计算过后的结果
                Integer sum = reducingState.get();

                out.collect(Tuple2.of(value.getId(),sum));
            }
        }).print();



        env.execute();
    }
}
