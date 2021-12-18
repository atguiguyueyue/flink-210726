package com.atguigu.day06;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class Flink08_StateBackend {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 状态后端的设置

        //TODO 内存级别
        //老版设置方法
        env.setStateBackend(new MemoryStateBackend());

        //新版设置方法
        env.setStateBackend(new HashMapStateBackend());
        //在新版本需要额外声明一下做Checkpoint时的存储位置
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());



        //TODO 文件级别的
        //老版本写法
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/....."));

        //新版本写法
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/.....");


        //TODO RocksDB
        //老版本写法
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/rocksDb/....."));

        //新版本写法
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/rocksDb/.....");

        //设置不对齐的Barrier
        env.getCheckpointConfig().enableUnalignedCheckpoints();


        env.execute();


    }
}
