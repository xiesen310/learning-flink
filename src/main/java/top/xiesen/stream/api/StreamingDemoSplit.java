package top.xiesen.stream.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import top.xiesen.stream.source.custom.MyNoParallelSource;

import java.util.ArrayList;

/**
 * @Description split 根据规则把一个流切分为多个流
 * <p>
 * 应用场景
 * 可能在实际工作中，源数据中混合了多种类似的数据，多种类型的数据处理规则不一样，所以就可以根据一定的规则，
 * 把一个数据流切分成多个数据流，这样每个数据流就可以使用不同的处理逻辑了
 * @className top.xiesen.stream.source.custom.StreamingDemoWithMyNoParallelismSource
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/9 20:20
 */
public class StreamingDemoSplit {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 此 Parallelism 只能设置为 1 , 只支持 Parallelism 为 1
        DataStreamSource<Long> text = env.addSource(new MyNoParallelSource()).setParallelism(1);

        // 对流进行切分, 按照数据的奇偶性进行区分
        SplitStream<Long> splitStream = text.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> output = new ArrayList<>();
                if (value % 2 == 0) {
                    output.add("even");// 偶数
                } else {
                    output.add("odd");// 奇数
                }
                return output;
            }
        });

        // 选择一个或者多个切分后的流
        DataStream<Long> evenStream = splitStream.select("even");
        DataStream<Long> oddStream = splitStream.select("odd");

        DataStream<Long> moreStream = splitStream.select("even", "odd");

        moreStream.rebalance();

        moreStream.print().setParallelism(1);

        String jobName = StreamingDemoSplit.class.getSimpleName();
        env.execute(jobName);
    }
}
