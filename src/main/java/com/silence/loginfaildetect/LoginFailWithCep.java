package com.silence.loginfaildetect;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LoginFailWithCep {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String path=Thread.currentThread().getContextClassLoader().getResource("LoginLog.csv").getPath();
        DataStreamSource<String> ds = env.readTextFile(path);

        SingleOutputStreamOperator<LoginEvent> mapDs = ds.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String s) throws Exception {
                String[] split = s.split(",");
                return new LoginEvent(Long.valueOf(split[0]), split[1], split[2], Long.valueOf(split[3]));
            }
        });
        SingleOutputStreamOperator<LoginEvent> waterDs = mapDs.assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent loginEvent, long l) {
                        return loginEvent.getTimestamp() * 1000;
                    }
                }));
        KeyedStream<LoginEvent, Long> keyedDs = waterDs.keyBy(new KeySelector<LoginEvent, Long>() {

            @Override
            public Long getKey(LoginEvent loginEvent) throws Exception {
                return loginEvent.getUserId();
            }
        });

        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("firstFail").where(
                new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.getLoginState().equals("fail");
                    }
                }
        ).next("secondFail").where(
                new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.getLoginState().equals("fail");
                    }
                }
        ).within(Time.seconds(2));

        PatternStream<LoginEvent> pattern1 = CEP.pattern(keyedDs, pattern);
        SingleOutputStreamOperator<LoginFailWarning> failDs = pattern1.flatSelect(new PatternFlatSelectFunction<LoginEvent, LoginFailWarning>() {
            @Override
            public void flatSelect(Map<String, List<LoginEvent>> map, Collector<LoginFailWarning> collector) throws Exception {
                List<LoginEvent> firstFail = map.get("firstFail");
                List<LoginEvent> secondFail = map.get("secondFail");

                for (int i = 0; i < firstFail.size(); i++) {
                    Long userId = firstFail.get(i).getUserId();
                    Long firstTs = firstFail.get(i).getTimestamp();
                    Long secondTs = secondFail.get(i).getTimestamp();
                    String msg = "fail~~~~~~";
                    collector.collect(new LoginFailWarning(userId, firstTs, secondTs, msg));
                }
            }
        });
        failDs.print();

        env.execute("loginfail");
    }

}
