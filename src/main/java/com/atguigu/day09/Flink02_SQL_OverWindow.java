package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink02_SQL_OverWindow {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int, " +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second)" +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor-sql.txt',"
                + "'format' = 'csv'"
                + ")"
        );

       //使用OverWindow
        tableEnv.executeSql("select\n" +
                " id,\n" +
                " ts,\n" +
                " vc,\n" +
                " count(vc) over w,\n" +
                " sum(vc) over w\n" +
                " from sensor\n" +
                " window w as (partition by id order by t rows between 1 PRECEDING and current row)").print();
    }
}
