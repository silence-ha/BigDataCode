package com.silence.ordertimeoutdetect;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class OrderTimeout {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String path=Thread.currentThread().getContextClassLoader().getResource("OrderLog.csv").getPath();
        DataStreamSource<String> ds = env.readTextFile(path);
        SingleOutputStreamOperator<OrderEvent> mapDs = ds.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String s) throws Exception {
                String[] str = s.split(",");
                return new OrderEvent(Long.valueOf(str[0]), str[1], str[2], Long.valueOf(str[3]));
            }
        });

        SingleOutputStreamOperator<OrderEvent> waterDs = mapDs.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent orderEvent, long l) {
                        return orderEvent.getTimestamp() * 1000;
                    }
                }));
        KeyedStream<OrderEvent, Long> keyedDs = waterDs.keyBy(new KeySelector<OrderEvent, Long>() {
            @Override
            public Long getKey(OrderEvent orderEvent) throws Exception {
                return orderEvent.getOrderId();
            }
        });

        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create").where(
                new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.getEventType().equals("create");
                    }
                }
        ).followedBy("pay").where(
                new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.getEventType().equals("pay");
                    }
                }
        ).within(Time.minutes(15));

        SingleOutputStreamOperator<OrderResult> cepDs = CEP.pattern(keyedDs, pattern)
                .select(new OutputTag<OrderResult>("timeOut") {
                }, new PatternTimeoutFunction<OrderEvent, OrderResult>() {
                    @Override
                    public OrderResult timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                        return new OrderResult(map.get("create").iterator().next().getOrderId(), "timeout~~~~~~~");
                    }
                }, new PatternSelectFunction<OrderEvent, OrderResult>() {
                    @Override
                    public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
                        return new OrderResult(map.get("create").iterator().next().getOrderId(), "nooo");
                    }
                });

        cepDs.getSideOutput(new OutputTag<OrderResult>("timeOut"){})
                .print();

        cepDs.print();

        env.execute("orderTimeout");
    }
}
