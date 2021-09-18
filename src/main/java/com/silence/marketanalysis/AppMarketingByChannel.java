package com.silence.marketanalysis;

import com.silence.hotitemanalysis.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class AppMarketingByChannel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<MarketingUserBehavior> ds = env.addSource(new SimulatedMarketingBehaviorSource());
        SingleOutputStreamOperator<MarketingUserBehavior> waterDs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.<MarketingUserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<MarketingUserBehavior>() {
                    @Override
                    public long extractTimestamp(MarketingUserBehavior marketingUserBehavior, long l) {
                        return marketingUserBehavior.getTimestamp();
                    }
                }));
        KeyedStream<MarketingUserBehavior, Tuple> keyedDS = waterDs.keyBy("channel", "behavior");

        SingleOutputStreamOperator<ChannelPromotionCount> aggDs = keyedDS.timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new AggregateFunction<MarketingUserBehavior, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0l;
                    }

                    @Override
                    public Long add(MarketingUserBehavior marketingUserBehavior, Long aLong) {
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
                }, new ProcessWindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Long> iterable, Collector<ChannelPromotionCount> collector) throws Exception {
                        long end = context.window().getEnd();
                        String str = new Timestamp(end).toString();
                        collector.collect(new ChannelPromotionCount(tuple.getField(0), tuple.getField(1), str, iterable.iterator().next()));
                    }
                });

        aggDs.print();

        env.execute("market");

    }
    public static class SimulatedMarketingBehaviorSource implements SourceFunction<MarketingUserBehavior>{

        boolean cancel=false;
        List<String> behaviorList  = Arrays.asList("CLICK","DOWNLOAD","INSTALL","UNINSTALL");
        List<String> channelList  = Arrays.asList("app store","weibo","wechat","tieba");
        Random random=new Random();
        @Override
        public void run(SourceContext<MarketingUserBehavior> sourceContext) throws Exception {
            while(!cancel){
                Long id=random.nextLong();
                String behavior = behaviorList.get(random.nextInt(4));
                String channel = channelList.get(random.nextInt(4));
                long ts = System.currentTimeMillis();
                Thread.sleep(50l);
                sourceContext.collect(new MarketingUserBehavior(id,behavior,channel,ts));
            }
        }

        @Override
        public void cancel() {
            cancel=true;
        }
    }
}
