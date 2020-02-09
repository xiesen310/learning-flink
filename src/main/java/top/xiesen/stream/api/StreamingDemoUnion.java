package top.xiesen.stream.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import top.xiesen.stream.source.custom.MyNoParallelSource;

/**
 * @Description union 演示，合并多个流，合并流的数据类型必须相同
 * @className top.xiesen.stream.source.custom.StreamingDemoWithMyNoParallelismSource
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/9 20:20
 */
public class StreamingDemoUnion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 此 Parallelism 只能设置为 1 , 只支持 Parallelism 为 1
        DataStreamSource<Long> text1 = env.addSource(new MyNoParallelSource()).setParallelism(1);
        DataStreamSource<Long> text2 = env.addSource(new MyNoParallelSource()).setParallelism(1);

        // 将 text1 和 text2 组装到一起
        DataStream<Long> text = text1.union(text2);
        DataStream<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("原始接收到的数据: " + value);
                return value;
            }
        });

        // 每2秒处理一次数据
        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        sum.print().setParallelism(1);

        String jobName = StreamingDemoUnion.class.getSimpleName();
        env.execute(jobName);
    }
}
