package com.silence.networkflowanalysis;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

public class HotPages {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String path=Thread.currentThread().getContextClassLoader().getResource("apache.log").getPath();

        DataStreamSource<String> ds = env.readTextFile(path);

        SingleOutputStreamOperator<ApacheLogEvent> mapDs = ds.map(new MapFunction<String, ApacheLogEvent>() {
            @Override
            public ApacheLogEvent map(String s) throws Exception {
                SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                String[] s1 = s.split(" ");
                long ts = sdf.parse(s1[3]).getTime();
                return new ApacheLogEvent(s1[0], s1[1], ts, s1[5], s1[6]);
            }
        });
        SingleOutputStreamOperator<ApacheLogEvent> tsDs = mapDs.assignTimestampsAndWatermarks(WatermarkStrategy.<ApacheLogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1)).
                withTimestampAssigner(new SerializableTimestampAssigner<ApacheLogEvent>() {

                    @Override
                    public long extractTimestamp(ApacheLogEvent apacheLogEvent, long l) {
                        return apacheLogEvent.getTimestamp();
                    }
                }));
        KeyedStream<ApacheLogEvent, String> keyedDS = tsDs
                .filter(new FilterFunction<ApacheLogEvent>() {
                    @Override
                    public boolean filter(ApacheLogEvent apacheLogEvent) throws Exception {
                        return apacheLogEvent.getMethod().equals("GET");
                    }
                })
                .keyBy(new KeySelector<ApacheLogEvent, String>() {
            @Override
            public String getKey(ApacheLogEvent apacheLogEvent) throws Exception {
                return apacheLogEvent.getUrl();
            }
        });

        SingleOutputStreamOperator<PageViewCount> aggDs = keyedDS.timeWindow(Time.seconds(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(new OutputTag<ApacheLogEvent>("later"){})
                .aggregate(new AggregateFunction<ApacheLogEvent, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0l;
                    }

                    @Override
                    public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
                        return aLong + 1l;
                    }

                    @Override
                    public Long getResult(Long aLong) {
                        return aLong;
                    }

                    @Override
                    public Long merge(Long aLong, Long acc1) {
                        return aLong + acc1;
                    }
                }, new WindowFunction<Long, PageViewCount, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
                        Long next = iterable.iterator().next();
                        long end = timeWindow.getEnd();

                        collector.collect(new PageViewCount(s, end, next));
                    }
                });

        SingleOutputStreamOperator<String> pageDs = aggDs.keyBy(new KeySelector<PageViewCount, Long>() {
            @Override
            public Long getKey(PageViewCount pageViewCount) throws Exception {
                return pageViewCount.getWindowEnd();
            }
        })
                .process(new KeyedProcessFunction<Long, PageViewCount, String>() {
                    ListState<PageViewCount> pageList;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        pageList = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("page", PageViewCount.class));
                    }

                    @Override
                    public void processElement(PageViewCount pageViewCount, Context context, Collector<String> collector) throws Exception {

                        pageList.add(pageViewCount);
                        context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

                        Iterator<PageViewCount> iterator = pageList.get().iterator();
                        ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(iterator);


                        pageViewCounts.sort(new Comparator<PageViewCount>() {
                            @Override
                            public int compare(PageViewCount o1, PageViewCount o2) {
                                if (o1.getCount() > o2.getCount()) {
                                    return 1;
                                } else {
                                    return 0;
                                }
                            }
                        });
                        StringBuilder sb = new StringBuilder();
                        if (pageViewCounts.size() > 3) {
                            for (int i = 0; i < 3; i++) {
                                sb.append(pageViewCounts.get(i).toString() + "\n");
                            }
                        } else {
                            for (int i = 0; i < pageViewCounts.size(); i++) {
                                sb.append(pageViewCounts.get(i).toString() + "\n");
                            }
                        }

                        System.out.println(sb.toString());
                        out.collect(sb.toString());
                    }
                });

        pageDs.print();
        env.execute("hot_page");
    }
}
