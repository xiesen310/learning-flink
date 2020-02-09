package top.xiesen.stream.source.custom;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Description 使用并行度为 1 的 source
 * @className top.xiesen.stream.source.custom.StreamingDemoWithMyNoParallelismSource
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/9 20:20
 */
public class StreamingDemoWithMyRichParallelSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> text = env.addSource(new MyRichParallelSource());
        DataStream<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到的数据: " + value);
                return value;
            }
        });

        // 每2秒处理一次数据
        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        sum.print().setParallelism(1);

        String jobName = StreamingDemoWithMyRichParallelSource.class.getSimpleName();
        env.execute(jobName);
    }
}
