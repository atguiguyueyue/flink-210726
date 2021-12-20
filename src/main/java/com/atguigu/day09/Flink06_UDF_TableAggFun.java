package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink06_UDF_TableAggFun {
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
//                .flatAggregate(call(MyUDTAF.class, $("vc")))
//                .select($("id"),$("f0").as("vc"),$("f1").as("RANK")).execute().print();
        //先注册再使用
        tableEnv.createTemporarySystemFunction("myTop2", MyUDTAF.class);
//
        table
             .groupBy($("id"))
                .flatAggregate(call("myTop2", $("vc")).as("vc", "Rank"))
                .select($("id"),$("vc"),$("Rank")).execute().print();

//        tableEnv.executeSql("select id,myAvg(vc) from "+table+" group by id").print();
    }

    //创建一个类作为累加器
    public static class Myacc{
        public Integer frist = Integer.MIN_VALUE;
        public Integer second =Integer.MIN_VALUE;
    }

    //自定义一个表聚合函数，多进多出，求vc最大的两个值
    public static class MyUDTAF extends TableAggregateFunction<Tuple2<Integer,String>,Myacc>{

        @Override
        public Myacc createAccumulator() {
            return new Myacc();
        }

        public void accumulate(Myacc acc,Integer value){
            if (value>acc.frist){
                acc.second = acc.frist;
                acc.frist = value;
            }else if (value>acc.second){
                acc.second = value;
            }

        }

        public void emitValue(Myacc acc, Collector<Tuple2<Integer,String>> out){
            if (acc.frist!=Integer.MIN_VALUE){

                out.collect(Tuple2.of(acc.frist,"1"));
            }

            if (acc.second!=Integer.MIN_VALUE){

                out.collect(Tuple2.of(acc.second,"2"));
            }

        }
    }

}
