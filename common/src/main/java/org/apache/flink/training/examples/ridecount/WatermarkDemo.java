package org.apache.flink.training.examples.ridecount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class WatermarkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Event> source = env.socketTextStream("localhost", 9999).map(new EventMapFunction());
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> withTimestampsAndWatermarks = source
                .assignTimestampsAndWatermarks(
                        new CustomWatermarkAssigner()
                );

        OutputTag<Event> lateTag = new OutputTag<Event>("late-tag") {
        };


        SingleOutputStreamOperator<String> windowResult = withTimestampsAndWatermarks
                .keyBy(event -> event.num)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sideOutputLateData(lateTag)
                .process(new ProcessWindowFunction<Event, String, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<Event> elements, Collector<String> out) {
                        TimeWindow window = context.window();
                        long watermark = context.currentWatermark();

                        // 收集窗口内所有事件
                        List<String> eventStrings = new ArrayList<>();
                        elements.forEach(event -> eventStrings.add(event.toString()));

                        // 格式化窗口时间
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        String windowStart = LocalDateTime
                                .ofInstant(Instant.ofEpochMilli(window.getStart()), ZoneId.systemDefault())
                                .format(formatter);

                        String windowEnd = LocalDateTime
                                .ofInstant(Instant.ofEpochMilli(window.getEnd()), ZoneId.systemDefault())
                                .format(formatter);

                        String result = String.format(
                                "窗口开始时间：%s，窗口结束时间：%s，watermark：%d，窗口内数据：%s",
                                windowStart, windowEnd, watermark, String.join(",", eventStrings)
                        );

                        out.collect(result);
                    }
                });

        windowResult.print();
        // 处理迟到数据
        DataStream<Event> lateStream = windowResult.getSideOutput(lateTag);
        lateStream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event event, Context ctx, Collector<String> out) {
                out.collect("迟到事件: " + event);
            }
        }).print();

        env.execute("Event Time Window Example");
    }

    private static class CustomWatermarkAssigner implements AssignerWithPeriodicWatermarks<Event> {

        private static final AtomicLong currentMaxTime = new AtomicLong(0L);
        private static final long timeDiff = 4000L;
        private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public long extractTimestamp(Event event, long l) {
            long eventTime = event.timestamp;
            // 使用CAS确保线程安全
            while (true) {
                long current = currentMaxTime.get();
                if (eventTime <= current) break;
                if (currentMaxTime.compareAndSet(current, eventTime)) break;
            }

            System.out.println("Event: " + event +
                    ", CurrentMaxTime: " + currentMaxTime.get() +
                    ", Watermark: " + (currentMaxTime.get() - timeDiff) +
                    " | " + sdf.format(currentMaxTime.get() - timeDiff));

            return eventTime;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTime.get() - timeDiff);
        }
    }
}



class EventMapFunction implements MapFunction<String, Event> {

    @Override
    public Event map(String s) throws Exception {
        String[] arr = s.split(",");
        return new Event(Long.parseLong(arr[0]), Long.parseLong(arr[1]));
    }
}

class Event {
    long num;

    long timestamp;

    Event(long num, long timestamp) {
        this.num = num;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return "Event{" +
                "num=" + num +
                ", timestamp=" + sdf.format(timestamp) +
                '}';
    }
}
