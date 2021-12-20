package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Flink07_HiveCatalog {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 3.创建HiveCatalog
        String name            = "myhive";  // Catalog 名字
        String defaultDatabase = "flink_test"; // 默认数据库
        String hiveConfDir     = "c:/conf"; // hive配置文件的目录. 需要把hive-site.xml添加到该目录

        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir);

        //TODO 4.注册Catalog
        tableEnv.registerCatalog(name, hiveCatalog);

        //指定使用哪个HiveCatalog
        tableEnv.useCatalog(name);

        //指定使用哪个数据库
        tableEnv.useDatabase(defaultDatabase);

        tableEnv.executeSql("select * from stu").print();
    }
}
