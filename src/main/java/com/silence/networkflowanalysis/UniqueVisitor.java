package com.silence.networkflowanalysis;

import com.google.common.collect.Lists;
import com.silence.hotitemanalysis.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class UniqueVisitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        SingleOutputStreamOperator<String> processDs = waterDs.filter(f -> f.getBehavior().equals("pv"))
                .map(new MapFunction<UserBehavior, Tuple2<String, UserBehavior>>() {
                    @Override
                    public Tuple2<String, UserBehavior> map(UserBehavior userBehavior) throws Exception {
                        return Tuple2.of("uv", userBehavior);
                    }
                })
                .keyBy(f -> f.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, UserBehavior>, String>() {
                    ListState<Long> userIdList;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        userIdList = getRuntimeContext().getListState(new ListStateDescriptor<Long>("userId", Long.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, UserBehavior> value, Context context, Collector<String> collector) throws Exception {
                        boolean b = userIdList.get().iterator().hasNext();
                        userIdList.add(value.f1.getUserId());
                        if (!b) {
                            context.timerService().registerEventTimeTimer((value.f1.getTimestamp() * 1000l / (24 * 60 * 60 * 1000) + 1) * 24 * 60 * 60 * 1000 - 8 * 60 * 60 * 1000);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        ArrayList<Long> list = Lists.newArrayList(userIdList.get().iterator());
                        Set<Long> set = new HashSet<Long>();
                        for (Long userId : list) {
                            set.add(userId);
                        }
                        String s = "uv:" + set.size();
                        out.collect(s);
                        userIdList.clear();
                    }
                });

        processDs.print();
        env.execute("uv");

    }
}
