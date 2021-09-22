package com.silence.loginfaildetect;

import com.silence.ordertimeoutdetect.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class TxPayMatch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String path=Thread.currentThread().getContextClassLoader().getResource("OrderLog.csv").getPath();
        DataStreamSource<String> orderDs = env.readTextFile(path);
        SingleOutputStreamOperator<OrderEvent> ordermapDs = orderDs.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String s) throws Exception {
                String[] str = s.split(",");
                return new OrderEvent(Long.valueOf(str[0]), str[1], str[2], Long.valueOf(str[3]));
            }
        });
        SingleOutputStreamOperator<OrderEvent> orderwaterDs = ordermapDs.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent orderEvent, long l) {
                        return orderEvent.getTimestamp() * 1000;
                    }
                })).filter( data -> ! "".equals(data.getTxId()) );

        KeyedStream<OrderEvent, String> orderkeyedDs = orderwaterDs.keyBy(new KeySelector<OrderEvent, String>() {
            @Override
            public String getKey(OrderEvent orderEvent) throws Exception {
                return orderEvent.getTxId();
            }
        });

        String path1=Thread.currentThread().getContextClassLoader().getResource("ReceiptLog.csv").getPath();
        DataStreamSource<String> receiptDs = env.readTextFile(path1);
        SingleOutputStreamOperator<ReceiptEvent> receiptmapDs = receiptDs.map(new MapFunction<String, ReceiptEvent>() {
            @Override
            public ReceiptEvent map(String s) throws Exception {
                String[] str = s.split(",");
                return new ReceiptEvent(str[0], str[1], Long.valueOf(str[2]));
            }
        });
        SingleOutputStreamOperator<ReceiptEvent> receiptwaterDs = receiptmapDs.assignTimestampsAndWatermarks(WatermarkStrategy.<ReceiptEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<ReceiptEvent>() {
                    @Override
                    public long extractTimestamp(ReceiptEvent receiptEvent, long l) {
                        return receiptEvent.getTimestamp() * 1000;
                    }
                }));
        KeyedStream<ReceiptEvent, String> receiptkeyedDs = receiptwaterDs.keyBy(new KeySelector<ReceiptEvent, String>() {
            @Override
            public String getKey(ReceiptEvent orderEvent) throws Exception {
                return orderEvent.getTxId();
            }
        });


        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resDs = orderkeyedDs.connect(receiptkeyedDs).process(new KeyedCoProcessFunction<String, OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>() {
            ValueState<OrderEvent> payState;
            ValueState<ReceiptEvent> receiptState;

            @Override
            public void open(Configuration parameters) throws Exception {
                payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("pay", OrderEvent.class));
                receiptState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receipt", ReceiptEvent.class));
            }

            @Override
            public void processElement1(OrderEvent orderEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
                if (receiptState.value() != null) {
                    collector.collect(Tuple2.of(orderEvent, receiptState.value()));
                    receiptState.clear();
                } else {
                    context.timerService().registerEventTimeTimer(orderEvent.getTimestamp() * 1000L + 5000L);
                    payState.update(orderEvent);
                }
            }

            @Override
            public void processElement2(ReceiptEvent receiptEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
                if (payState.value() != null) {
                    collector.collect(Tuple2.of(payState.value(), receiptEvent));
                    payState.clear();
                } else {
                    context.timerService().registerEventTimeTimer(receiptEvent.getTimestamp() * 1000L + 3000L);
                    receiptState.update(receiptEvent);
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
                if (payState.value() != null) {
                    ctx.output(new OutputTag<OrderEvent>("pay") {
                    }, payState.value());
                }
                if (receiptState.value() != null) {
                    ctx.output(new OutputTag<ReceiptEvent>("receipt") {
                    }, receiptState.value());
                }

                payState.clear();
                receiptState.clear();
            }
        });

        resDs.getSideOutput(new OutputTag<OrderEvent>("pay"){} )
                .print("no match pay");
        resDs.getSideOutput(new OutputTag<ReceiptEvent>("receipt"){} )
                .print("no match receipt");

        resDs.print("match");

        env.execute();
    }
}
