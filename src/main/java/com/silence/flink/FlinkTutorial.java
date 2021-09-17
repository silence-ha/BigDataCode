package com.silence.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.net.URL;

public class FlinkTutorial {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        String inputPath= "hello.txt";
        URL resource = Thread.currentThread().getContextClassLoader().getResource("hello.txt");

        DataStreamSource<String> ds = env.readTextFile(resource.getPath());

        SingleOutputStreamOperator<String> word = ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String str : s.split(" ")) {
                    collector.collect(str);
                }

            }
        });
         SingleOutputStreamOperator<Tuple2<String, Integer>> wc = word.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.<String, Integer>of(s, 1);
            }
        });

         wc.print();

         env.execute("wc");
    }
}
