package top.xiesen.stream.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import top.xiesen.stream.source.custom.MyNoParallelSource;

/**
 * @Description connect 和 union 类似，但是只能连接两个流，两个流的数据类型可以不同，会对两个流中的数据应用不同的处理方法
 * @className top.xiesen.stream.source.custom.StreamingDemoWithMyNoParallelismSource
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/9 20:20
 */
public class StreamingDemoConnect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 此 Parallelism 只能设置为 1 , 只支持 Parallelism 为 1
        DataStreamSource<Long> text1 = env.addSource(new MyNoParallelSource()).setParallelism(1);
        DataStreamSource<Long> text2 = env.addSource(new MyNoParallelSource()).setParallelism(1);

        DataStream<String> text2Str = text2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "str" + value;
            }
        });

        ConnectedStreams<Long, String> connectStream = text1.connect(text2Str);

        SingleOutputStreamOperator<Object> result = connectStream.map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Object map1(Long value) throws Exception {
                return value;
            }

            @Override
            public Object map2(String value) throws Exception {
                return value;
            }
        });

        result.print().setParallelism(1);

        String jobName = StreamingDemoConnect.class.getSimpleName();
        env.execute(jobName);
    }
}
