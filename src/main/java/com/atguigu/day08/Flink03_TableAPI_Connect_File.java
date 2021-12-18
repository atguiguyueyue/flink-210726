package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.api.Expressions.$;

public class Flink03_TableAPI_Connect_File {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 3.连接外部文件系统，获取数据，并将其映射为临时表

        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        tableEnv.connect(new FileSystem().path("input/sensor-sql.txt"))
                .withFormat(new Csv().lineDelimiter("\n").fieldDelimiter(','))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //查询临时表中的数据
//        TableResult tableResult = tableEnv.executeSql("select * from sensor");
//        tableResult.print();


//        Table table = tableEnv.sqlQuery("select * from sensor");
//        TableResult result = table.execute();
//        result.print();

        Table table = tableEnv.from("sensor");
        Table table1 = table
                .groupBy($("id"))
                .select($("id"), $("vc").sum());
        table1.execute().print();

    }
}
