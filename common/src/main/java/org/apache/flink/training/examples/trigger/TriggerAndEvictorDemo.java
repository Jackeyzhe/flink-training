package org.apache.flink.training.examples.trigger;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TriggerAndEvictorDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Double>> source = env.socketTextStream("localhost", 9999).map(new MapFunction<String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String s) throws Exception {
                String[] arr = s.split(",");
                return Tuple2.of(arr[0], Double.valueOf(arr[1]));
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple2<String, Double>>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // 允许5秒的乱序
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Double>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Double> element, long recordTimestamp) {
                                return System.currentTimeMillis();
                            }
                        })
        );
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        source.keyBy(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(5L)))
                .trigger(new Trigger<Tuple2<String, Double>, TimeWindow>() {
                    private final double threshold = 10.0;
                    private final int countThreshold = 5;
                    private final String stateName = "monitor";

                    @Override
                    public TriggerResult onElement(Tuple2<String, Double> item, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        ValueState<Integer> state = triggerContext.getPartitionedState(new ValueStateDescriptor<>(stateName, Integer.class, 0));

                        if (item.f1 > threshold) {
                            int count = state.value() + 1;
                            state.update(count);

                            if (count >= countThreshold) {
                                state.clear();
                                return TriggerResult.FIRE;
                            }
                        } else {
                            state.clear();
                        }
                        return TriggerResult.CONTINUE;
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
                        triggerContext.getPartitionedState(new ValueStateDescriptor<>(stateName, Integer.class)).clear();
                    }
                })
                .evictor(new Evictor<Tuple2<String, Double>, TimeWindow>() {
                    private final int maxSize = 10;

                    @Override
                    public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Double>>> elements, int size, TimeWindow timeWindow, EvictorContext evictorContext) {

                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Double>>> elements, int size, TimeWindow timeWindow, EvictorContext evictorContext) {
                        if (size <= maxSize) {
                            return;
                        }

                        List<TimestampedValue<Tuple2<String, Double>>> elementList = new ArrayList<>();
                        elements.forEach(elementList::add);
                        int toEvict = size - maxSize;
                        List<TimestampedValue<Tuple2<String, Double>>> toRemove = elementList.subList(maxSize, size);

                        // 4. 通过迭代器删除需要移除的元素（关键：遍历原始迭代器，匹配并删除）
                        Iterator<TimestampedValue<Tuple2<String, Double>>> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            TimestampedValue<Tuple2<String, Double>> element = iterator.next();
                            if (toRemove.contains(element)) { // 匹配需要删除的元素
                                iterator.remove(); // 实际删除
                                toEvict--;
                                if (toEvict <= 0) {
                                    break;
                                }
                            }
                        }

                        System.out.println("Evictor finished: 保留了 " + maxSize + " 个元素");

                    }
                }).apply(new WindowFunction<Tuple2<String, Double>, String, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<Tuple2<String, Double>> iterable, Collector<String> collector) throws Exception {
                        System.out.println("send alarm!");
                        List<Tuple2<String, Double>> elements = new ArrayList<>();
                        iterable.forEach(elements::add);
                        for (Tuple2<String, Double> element : elements) {
                            collector.collect("Element: " + element);
                        }
                    }
                });
        env.execute();
    }
}
