package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink01_SQL_GroupWindow {
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

        //开启一个滚动窗口
//        tableEnv.executeSql("SELECT id, " +
//                "  TUMBLE_START(t, INTERVAL '2' second) as wStart,  " +
//                "  TUMBLE_END(t, INTERVAL '2' second) as wEnd,  " +
//                "  SUM(vc) sum_vc " +
//                "FROM sensor " +
//                "GROUP BY TUMBLE(t, INTERVAL '2' second), id"
//        ).print();

        //开启一个滑动窗口
//        tableEnv.executeSql("SELECT id, " +
//                "  HOP_START(t, INTERVAL '2' second,INTERVAL '3' second) as wStart,  " +
//                "  HOP_END(t, INTERVAL '2' second,INTERVAL '3' second) as wEnd,  " +
//                "  SUM(vc) sum_vc " +
//                "FROM sensor " +
//                "GROUP BY HOP(t, INTERVAL '2' second,INTERVAL '3' second), id"
//        ).print();
        //会话窗口
        tableEnv.executeSql("SELECT id, " +
                "  SESSION_START(t, INTERVAL '2' second) as wStart,  " +
                "  SESSION_END(t, INTERVAL '2' second) as wEnd,  " +
                "  SUM(vc) sum_vc " +
                "FROM sensor " +
                "GROUP BY SESSION(t, INTERVAL '2' second), id"
        ).print();
    }
}
