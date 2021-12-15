package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

public class Flink02_KeyedState_ListState {
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

        //5.针对每个传感器输出最高的3个水位值
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            //创建listState用来保存最高的三个水位
            private ListState<Integer> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("list-state", Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //1.将当前的水位保存至状态中
                listState.add(value.getVc());

                //2.将状态中的数据取出来，排序
                Iterable<Integer> vcIter = listState.get();

                //3.创建一个list集合用来保存迭代器中的数据
                ArrayList<Integer> listVc = new ArrayList<>();
                for (Integer integer : vcIter) {
                    listVc.add(integer);
                }

                //4.对集合中的数据由大到小排序
                listVc.sort(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o2 - o1;
                    }
                });

                //5.判断集合中的元素个数是否大于三，大于三删除最后一个（即最小的）
                if (listVc.size() > 3) {
                    listVc.remove(3);
                }

                //6.将list集合中的数据更新至状态中
                listState.update(listVc);

                out.collect(listVc.toString());

            }
        }).print();



        env.execute();
    }
}
