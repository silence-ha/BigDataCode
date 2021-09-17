package com.silence.hotitemanalysis;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

public class HotItems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        String path=Thread.currentThread().getContextClassLoader().getResource("UserBehavior.csv").getPath();
        DataStreamSource<String> ds = env.readTextFile(path);

        SingleOutputStreamOperator<UserBehavior> mapDs = ds.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                String[] strs = s.split(",");
                return new UserBehavior(Long.valueOf(strs[0]), Long.valueOf(strs[1]), Integer.valueOf(strs[2]), strs[3], Long.valueOf(strs[4]));
            }
        });
        SingleOutputStreamOperator<UserBehavior> waterDs = mapDs.assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior userBehavior, long l) {
                        return userBehavior.getTimestamp() * 1000L;
                    }
                }));
        final SingleOutputStreamOperator<UserBehavior> filterDs = waterDs.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.getBehavior().equals("pv");
            }
        });
        KeyedStream<UserBehavior, Tuple> keyedDs = filterDs.keyBy("itemId");
        WindowedStream<UserBehavior, Tuple, TimeWindow> timeDs = keyedDs.timeWindow(Time.minutes(60), Time.minutes(5));
        SingleOutputStreamOperator<ItemViewCount> aggDs = timeDs.aggregate(new AggregateFunction<UserBehavior, Long, Long>() {
            @Override
            public Long createAccumulator() {
                return 0l;
            }

            @Override
            public Long add(UserBehavior userBehavior, Long aLong) {
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
        }, new WindowFunction<Long, ItemViewCount, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
                Long next = iterable.iterator().next();
                long end = timeWindow.getEnd();
                collector.collect(new ItemViewCount(tuple.getField(0), end, next));
            }
        });

        SingleOutputStreamOperator<String> topnDs = aggDs.keyBy(new KeySelector<ItemViewCount, Long>() {
            @Override
            public Long getKey(ItemViewCount itemViewCount) throws Exception {
                return itemViewCount.getWindowEnd();
            }
        }).process(new KeyedProcessFunction<Long, ItemViewCount, String>() {
            ListState<ItemViewCount> topn;

            @Override
            public void open(Configuration parameters) throws Exception {
                topn = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("topn", ItemViewCount.class));

            }

            @Override
            public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
                topn.add(itemViewCount);
                context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                Iterator<ItemViewCount> iterator = topn.get().iterator();
                ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(iterator);
                itemViewCounts.sort(new Comparator<ItemViewCount>() {
                    @Override
                    public int compare(ItemViewCount o1, ItemViewCount o2) {
                        if (o1.getCount() > o2.getCount()) {
                            return 1;
                        } else {
                            return 0;
                        }

                    }
                });
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 3; i++) {
                    sb.append(itemViewCounts.get(i).toString()+"\n");
                }
                sb.append("=====================");
                Thread.sleep(1000);
                out.collect(sb.toString());
                topn.clear();
            }
        });
        topnDs.print();
        env.execute("topN");
    }
}
