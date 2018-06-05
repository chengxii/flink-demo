package com.cx.wc;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by xi.cheng on 2018/6/5.
 */
public class VolumeCountTest {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSource<String> source = env.readTextFile("D://1600data.txt");
        source
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String line) throws Exception {
                        String[] split = line.split("--");
                        if (split.length > 4) {
                            return true;
                        }
                        return false;
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String line) throws Exception {
                        String[] split = line.split("--");
                        return new Tuple2(split[1], Integer.parseInt(split[2]) / 1000 * Integer.parseInt(split[3]));
                    }
                })
                // .groupBy(0)
                .sum(1)
                .print();
    }
}
