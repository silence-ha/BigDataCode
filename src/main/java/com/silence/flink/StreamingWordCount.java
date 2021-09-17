package com.silence.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class StreamingWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host=parameterTool.get("host");
        int port=parameterTool.getInt("port");

        DataStreamSource<String> ds = env.socketTextStream(host, port);

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

        KeyedStream<Tuple2<String, Integer>, Object> kedStream = wc.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = kedStream.sum(1);

        sum.print();

        env.execute("wc");
    }
}
