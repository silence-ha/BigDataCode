package com.silence.marketanalysis;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

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

        SingleOutputStreamOperator<AdClickEvent> processDs = waterDs.keyBy("userId", "adId")
                .process(new KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent>() {
                    ValueState<Long> val;
                    ValueState<Boolean> isSentState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        val = getRuntimeContext().getState(new ValueStateDescriptor<Long>("val", Long.class,0l));
                        isSentState = getRuntimeContext().getState(new
                                ValueStateDescriptor<Boolean>("is-sent", Boolean.class, false));
                    }

                    @Override
                    public void processElement(AdClickEvent adClickEvent, Context context, Collector<AdClickEvent> collector) throws Exception {
                        Long c = val.value();
                        if (c == 0) {
                            val.update(c + 1);
                            Long ts = adClickEvent.getTimestamp() * 1000 + 60 * 60 * 1000;
                            context.timerService().registerEventTimeTimer(ts);
                            collector.collect(adClickEvent);
                        } else if (c < 100) {
                            val.update(c + 1);
                            collector.collect(adClickEvent);
                        } else {
                            if (!isSentState.value()) {
                                isSentState.update(true);
                                context.output(new OutputTag<AdClickEvent>("blacklist") {
                                }, adClickEvent);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
                        val.clear();
                        isSentState.clear();
                    }
                });
        processDs.getSideOutput(new OutputTag<AdClickEvent>("blacklist") {})
        .print("black------");


        KeyedStream<AdClickEvent, String> keyedDs = processDs.keyBy(new KeySelector<AdClickEvent, String>() {

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
