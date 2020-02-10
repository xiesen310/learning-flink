package top.xiesen.stream.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * 对于延迟数据的处理方式:
 * 1. 丢弃数据
 * 2. 设置延时时间, 延时时间设置过大，影响程序性能
 * 3. 存储起来，做数据分析
 * <p>
 * window 设置延时时间触发条件:
 * 1. watermark 时间 >= window_end_time
 * 2. 在 [window_start_time,window_end_time) 区间中有数据存在, 注意是左闭右开的区间
 * 对于窗口而言，允许2秒的延迟数据，即第一次触发是在 watermark > window_end_time时
 * 第二次(或多次) 触发的条件是 watermark < window_end_time + allowedLateness 时间内，这个窗口有迟到数据到达时
 *
 * @Description StreamingWindowWatermark
 * @className top.xiesen.stream.watermark.StreamingWindowWatermark
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/10 14:44
 */
public class StreamingWindowWatermarkWithAllowedLateness {
    public static void main(String[] args) throws Exception {
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.out.println("No port set. use default port 9000");
            port = 9000;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置使用 eventTime, 默认使用 processTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 设置并行度为 1, 默认并行度是当前机器的 cpu 数量
        env.setParallelism(1);

        String hostname = "192.168.0.106";
        String delimiter = "\n";

        // 链接 socket 获取输入数据
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);

        DataStream<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });

        // 抽取 timestamp 和生成 watermark
        DataStream<Tuple2<String, Long>> watermarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            /**
             * 定义生成 watermark 逻辑
             * 默认 100ms 被调用一次
             */
            Long currentMaxTimestamp = 0L;
            final Long maxOutofOrderness = 10000L; // 最大允许的乱序时间 10s
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutofOrderness);
            }

            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                long timestamp = element.f1;
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                long id = Thread.currentThread().getId();
                System.out.println("当前线程id: " + id + " ,key:" + element.f0 + " ,eventTime:[" + element.f1 + "|" + sdf.format(element.f1) +
                        "],currentMaxTimestamp:[" + currentMaxTimestamp + "|" + sdf.format(currentMaxTimestamp) + "],watermark:[" +
                        getCurrentWatermark().getTimestamp() + "|" + sdf.format(getCurrentWatermark().getTimestamp()) + "]");
                return timestamp;
            }
        });

        DataStream<String> windowData = watermarkStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3))) // 按照消息的 eventTime分配窗口,和 调用 TimeWindow效果是一样的
                .allowedLateness(Time.seconds(2)) // 允许数据延迟 2s
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    /**
                     * 对 window 内的数据进行排序,保证数据的顺序
                     * @param tuple
                     * @param window
                     * @param input
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        String key = tuple.toString();
                        ArrayList<Long> arrayList = new ArrayList<>();
                        Iterator<Tuple2<String, Long>> it = input.iterator();
                        while (it.hasNext()) {
                            Tuple2<String, Long> next = it.next();
                            arrayList.add(next.f1);
                        }
                        Collections.sort(arrayList);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = key + "," + arrayList.size() + "," + sdf.format(arrayList.get(0)) + "," +
                                sdf.format(arrayList.get(arrayList.size() - 1)) + "," + sdf.format(window.getStart())
                                + "," + sdf.format(window.getEnd());
                        out.collect(result);
                    }
                });
        // 测试-把结果打印到控制台
        windowData.print();
        env.execute(StreamingWindowWatermarkWithAllowedLateness.class.getSimpleName());
    }
}
