package top.xiesen.stream.api;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import top.xiesen.stream.source.custom.MyNoParallelSource;

/**
 * @Description 使用并行度为 1 的 source
 * @className top.xiesen.stream.source.custom.StreamingDemoWithMyNoParallelismSource
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/9 20:20
 */
public class StreamingDemoFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 此 Parallelism 只能设置为 1 , 只支持 Parallelism 为 1
        DataStreamSource<Long> text = env.addSource(new MyNoParallelSource()).setParallelism(1);

        DataStream<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("原始接收到的数据: " + value);
                return value;
            }
        });

        // 执行过滤，满足条件的数据会被留下
        DataStream<Long> filterData = num.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        });

        DataStream<Long> resultData = filterData.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("过滤之后的数据: " + value);
                return value;
            }
        });

        // 每2秒处理一次数据
        DataStream<Long> sum = resultData.timeWindowAll(Time.seconds(2)).sum(0);
        sum.print().setParallelism(1);

        String jobName = StreamingDemoFilter.class.getSimpleName();
        env.execute(jobName);
    }
}
