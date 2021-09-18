package com.silence.networkflowanalysis;

import com.silence.hotitemanalysis.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;


import java.time.Duration;

public class UniqueVistorWithBloom {
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
        waterDs.timeWindowAll(Time.hours(1))
                .trigger(new Trigger<UserBehavior, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(UserBehavior userBehavior, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return TriggerResult.FIRE_AND_PURGE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

                    }
                })
                .process(new ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow>() {
                    Jedis jedis;
                    MyBloomFilter myBloomFilter;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        jedis=new Jedis("hadoop102",6379);
                        myBloomFilter=new MyBloomFilter(1<<29);
                    }

                    @Override
                    public void process(Context context, Iterable<UserBehavior> iterable, Collector<PageViewCount> collector) throws Exception {
                        String userId=iterable.iterator().next().getUserId().toString();
                        long end=context.window().getEnd();
                        String bitMapKey = "uvmap:"+String.valueOf(end);
                        String uvCountKey = String.valueOf(end);
                        String uvName="uvcount";
                        Long aLong = myBloomFilter.hashCode(userId, 61);
                        Boolean getbit = jedis.getbit(bitMapKey, aLong);
                        if(!getbit){
                            jedis.setbit(bitMapKey,aLong,true);
                            String uv = jedis.hget(uvName, uvCountKey);
                            if(uv !=null && !"".equals(uv)){
                                jedis.hset(uvName, uvCountKey,String.valueOf(Long.valueOf(uv)+1));
                            }else{
                                jedis.hset(uvName, uvCountKey,String.valueOf(1));
                            }
                        }
                    }

                    @Override
                    public void close() throws Exception {
                        jedis.close();
                    }
                });

        env.execute("uvwithBloom");
    }
    public static class MyBloomFilter{
        Integer cap;
        public MyBloomFilter(Integer cap){
            this.cap=cap;
        }
        public Long hashCode(String value,Integer seed){
            Long result=0L;
            for(int i=0;i<value.length();i++){
                result=result*seed+value.charAt(i);
            }

            return result & (cap-1);
        }
    }

}
