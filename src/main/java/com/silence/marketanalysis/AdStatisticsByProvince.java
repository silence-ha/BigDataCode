package com.silence.marketanalysis;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class AdStatisticsByProvince {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String path=Thread.currentThread().getContextClassLoader().getResource("AdClickLog.csv").getPath();
        DataStreamSource<String> ds = env.readTextFile(path);
        SingleOutputStreamOperator<AdClickEvent> mapDs = ds.map(new MapFunction<String, AdClickEvent>() {
            @Override
            public AdClickEvent map(String s) throws Exception {
                String str[] = s.split(",");
                return new AdClickEvent(Long.valueOf(str[0]), Long.valueOf(str[1]), str[2], str[3], Long.valueOf(str[4]));
            }
        });

        SingleOutputStreamOperator<AdClickEvent> waterDs = mapDs.assignTimestampsAndWatermarks(WatermarkStrategy.<AdClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<AdClickEvent>() {

                    @Override
                    public long extractTimestamp(AdClickEvent adClickEvent, long l) {
                        return adClickEvent.getTimestamp() * 1000l;
                    }
                }));
        KeyedStream<AdClickEvent, String> keyedDs = waterDs.keyBy(new KeySelector<AdClickEvent, String>() {

            @Override
            public String getKey(AdClickEvent adClickEvent) throws Exception {
                return adClickEvent.getProvince();
            }
        });
        SingleOutputStreamOperator<AdCountByProvince> aggDs = keyedDs.timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new AggregateFunction<AdClickEvent, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0l;
                    }

                    @Override
                    public Long add(AdClickEvent adClickEvent, Long aLong) {
                        return aLong + 1;
                    }

                    @Override
                    public Long getResult(Long aLong) {
                        return aLong;
                    }

                    @Override
                    public Long merge(Long aLong, Long acc1) {
                        return aLong + acc1;
                    }
                }, new ProcessWindowFunction<Long, AdCountByProvince, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Long> iterable, Collector<AdCountByProvince> collector) throws Exception {
                        Long next = iterable.iterator().next();
                        Long end = context.window().getEnd();
                        collector.collect(new AdCountByProvince(s, String.valueOf(end), next));
                    }
                });

        aggDs.print();

        env.execute("adpro");

    }
}
