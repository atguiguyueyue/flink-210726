package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink05_KeyedState_MapState {
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

        //5.去掉重复的水位值
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {

            //创建MapState状态
            private MapState<Integer, WaterSensor> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, WaterSensor>("map-satae", Integer.class, WaterSensor.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //1.判断key是否存在
                if (!mapState.contains(value.getVc())){
                    //不存在存进去
                    mapState.put(value.getVc(), value);
                }

                out.collect(mapState.get(value.getVc()).toString());
            }
        }).print();



        env.execute();
    }
}
