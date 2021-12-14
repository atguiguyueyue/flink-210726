package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Flink11_Timer_Exec {
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

        //将相同key的数据聚和到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");

        OutputTag<String> outputTag = new OutputTag<String>("报警信息") {};

        //监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，并将报警信息输出到侧输出流。
        SingleOutputStreamOperator<WaterSensor> process = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
            //定义一个变量用来保存上一次的水位
            private Integer lastVc = Integer.MIN_VALUE;

            //定义一个变量用来保存定时器时间
            private Long timer = Long.MIN_VALUE;

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //1.判断水位是否上升
                if (value.getVc()>lastVc){
                    //2.水位上升，判断定时器是否被注册，如果没被注册则注册定时器
                    if (timer==Long.MIN_VALUE){
                        //定时器没有注册
                        //3.注册定时器
                        System.out.println("注册定时器:"+ctx.getCurrentKey());
                        timer = ctx.timerService().currentProcessingTime() + 5000;
                        ctx.timerService().registerProcessingTimeTimer(timer);
                    }
                }else {
                    //水位没有上升
                    //4.取消之前已经注册过的定时器
                    System.out.println("删除定时器:"+ctx.getCurrentKey());
                    ctx.timerService().deleteProcessingTimeTimer(timer);

                    //5.为了方便下次水位上升时注册，因此要把定时器恢复成默认值
                    timer = Long.MIN_VALUE;
                }

                //6.无论水位是否变化，都要将本次的水位保存起来，以供下一次做对比
                lastVc = value.getVc();

                out.collect(value);
            }

            //7.定时器触发之后，为了让下5秒的数据能够重新注册定时器，恢复定时器时间
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                //恢复定时器时间
                timer = Long.MIN_VALUE;

                //获取侧输出流，将报警信息打印到侧输出流中
                ctx.output(outputTag, "警报！！！连续5s水位上升");

            }
        });

        process.print("主流");
        process.getSideOutput(outputTag).print("警报");

        env.execute();

    }
}
