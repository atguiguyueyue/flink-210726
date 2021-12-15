package com.atguigu.day06;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class Flink07_OperaterState_Broadcast {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取两条流
        DataStreamSource<String> localhostStream = env.socketTextStream("localhost", 9999);

        DataStreamSource<String> hadoopStream = env.socketTextStream("hadoop102", 9999);

        //3.定义一个状态并广播
        MapStateDescriptor<String, String> mapState = new MapStateDescriptor<>("map", String.class, String.class);

        //4.广播状态
        BroadcastStream<String> broadcast = localhostStream.broadcast(mapState);

        //5.连接普通流和广播流
        BroadcastConnectedStream<String, String> connect = hadoopStream.connect(broadcast);

        //6.对两条流的数据进行比对处理
        connect.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //提取广播状态
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapState);
                String aSwitch = broadcastState.get("switch");

                if ("1".equals(aSwitch)) {
                    out.collect("执行逻辑1.。。。。。");
                } else if ("2".equals(aSwitch)) {
                    out.collect("执行逻辑2.。。。。。。");
                } else {
                    out.collect("执行其他逻辑");
                }

            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                //提取状态
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapState);
                //将数据保存至状态中
                broadcastState.put("switch", value);
            }
        }).print();

        env.execute();


    }
}
