package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink05_UDF_AggFun {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取端口中的数据
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

        //3.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //4.将流转为表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //不注册直接使用
//        table
//             .groupBy($("id"))
//                .select($("id"),call(MyUDAF.class,$("vc"))).execute().print();
        //先注册再使用
        tableEnv.createTemporarySystemFunction("myAvg", MyUDAF.class);
//
//        table
//             .groupBy($("id"))
//                .select($("id"),call("myAvg",$("vc"))).execute().print();

        tableEnv.executeSql("select id,myAvg(vc) from "+table+" group by id").print();
    }

    //创建一个类作为累加器
    public static class Myacc{
        public Integer sum ;
        public Integer count;
    }

    //自定义一个聚合函数，多进一出，求vc平均值
    public static class MyUDAF extends AggregateFunction<Double,Myacc>{

        @Override
        public Myacc createAccumulator() {
            Myacc myacc = new Myacc();
            myacc.sum = 0;
            myacc.count = 0;
            return myacc;
        }

        public void accumulate(Myacc acc,Integer value){
            acc.sum += value;
            acc.count++;
        }

        @Override
        public Double getValue(Myacc accumulator) {
            if (accumulator.count==0){
                return null;
            }else {
                return accumulator.sum * 1D / accumulator.count;
            }
        }


    }

}
