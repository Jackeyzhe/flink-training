package org.apache.flink.training.examples.join;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WindowJoinDemo {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Double>> source1 = env.socketTextStream("localhost", 9999).map(new MapFunction<String, Tuple2<String, Double>>() {
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

        DataStream<Tuple2<String, Double>> source2 = env.socketTextStream("localhost", 9998).map(new MapFunction<String, Tuple2<String, Double>>() {
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

        DataStream<Tuple2<String, Double>> result = source1.join(source2)
                .where(record -> record.f0)
                .equalTo(record -> record.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(2L)))
                .apply(new JoinFunction<Tuple2<String, Double>, Tuple2<String, Double>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> join(Tuple2<String, Double> record1, Tuple2<String, Double> record2) throws Exception {
                        return Tuple2.of(record1.f0, record1.f1);
                    }
                });

DataStream<Tuple2<String, Double>> intervalJoinResult = source1.keyBy(record -> record.f0)
        .intervalJoin(source2.keyBy(record -> record.f0))
        .between(Time.seconds(-2), Time.seconds(2))
        .process(new ProcessJoinFunction<Tuple2<String, Double>, Tuple2<String, Double>, Tuple2<String, Double>>() {
            @Override
            public void processElement(Tuple2<String, Double> record1, Tuple2<String, Double> record2, ProcessJoinFunction<Tuple2<String, Double>, Tuple2<String, Double>, Tuple2<String, Double>>.Context context, Collector<Tuple2<String, Double>> out) throws Exception {
                out.collect(Tuple2.of(record1.f0, record1.f1 + record2.f1));
            }
        });
        result.print();

    }

private static class LeftJoinFunction implements CoGroupFunction<Tuple2<String, Double>, Tuple2<String, Double>, Tuple2<String, Double>> {

    @Override
    public void coGroup(Iterable<Tuple2<String, Double>> iterable1, Iterable<Tuple2<String, Double>> iterable2, Collector<Tuple2<String, Double>> collector) throws Exception {
        for (Tuple2<String, Double> record1 : iterable1) {
            boolean match = false;
            for (Tuple2<String, Double> record2 : iterable2) {
                match = true;
                collector.collect(Tuple2.of(record1.f0, record1.f1 + record2.f1));
            }
            if (!match) {
                System.out.println("没有join的元素 key:" + record1.f0);
                collector.collect(Tuple2.of(record1.f0, record1.f1));
            }
        }
    }
}
}


