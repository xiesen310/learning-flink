package top.xiesen.stream.custompartition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.xiesen.stream.source.custom.MyNoParallelSource;

/**
 * @Description 使用自定义分区
 * 根据数字的奇偶性来分区
 * @className top.xiesen.stream.custompartition.StreamingDemoWithMyPartition
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/9 22:07
 */
public class StreamingDemoWithMyPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<Long> text = env.addSource(new MyNoParallelSource());

        // 对数据进行转换，把 long 类型转换成 Tuple1<Long> 类型
        DataStream<Tuple1<Long>> tupleData = text.map(new MapFunction<Long, Tuple1<Long>>() {

            @Override
            public Tuple1<Long> map(Long value) throws Exception {
                return new Tuple1<>(value);
            }
        });

        // 分区之后的数据
        DataStream<Tuple1<Long>> partitionData = tupleData.partitionCustom(new MyPartition(), 0);

        DataStream<Long> result = partitionData.map(new MapFunction<Tuple1<Long>, Long>() {

            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("当前线程id: " + Thread.currentThread().getId() + ", value: " + value);
                return value.getField(0);
            }
        });

        result.print().setParallelism(1);

        String jobName = StreamingDemoWithMyPartition.class.getSimpleName();
        env.execute(jobName);
    }
}
