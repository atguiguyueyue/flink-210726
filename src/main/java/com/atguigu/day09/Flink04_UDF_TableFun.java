package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink04_UDF_TableFun {
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
        table
             .joinLateral(call(MyUDTF.class,$("id")))
                .select($("id"),$("f0")).execute().print();
        //先注册再使用
//        tableEnv.createTemporarySystemFunction("myExplor", MyUDTF.class);
//
//        table
//             .joinLateral(call("myExplor",$("id")).as("newWord"))
//                .select($("id"),$("newWord")).execute().print();

//        tableEnv.executeSql("select id,word from "+table+" join lateral table(myExplor(id)) on true").print();
//        tableEnv.executeSql("select id,word from "+table+",lateral table(myExplor(id))").print();
    }

    //自定义一个表函数，一进多出，根据id按照下划线切分出多个数据
//    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
    public static class MyUDTF extends TableFunction<Tuple1<String>>{
        public void eval(String value){
            String[] split = value.split("_");
            for (String s : split) {
                collect(Tuple1.of(s));
            }
        }
    }

}
