package top.xiesen.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Description 多数据源 join
 * @className top.xiesen.stream.DataSourceUnion
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/7 19:35
 */
public class DataSourceUnion {
    public static void main(String[] args) throws Exception {
        // 解析命令行参数
        ParameterTool params = ParameterTool.fromArgs(args);

        // 获取执行参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream1 = env.fromElements("flink flink flink");
        DataStream<String> dataStream2 = env.fromElements("spark spark spark");

        // 数据处理
        DataStream<Tuple2<String, Integer>> counts = dataStream1.union(dataStream2).flatMap(new WordCountStream.Tokenizer())
                .keyBy(0)
                .sum(1);

        // 输出统计结果
        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
        } else {
            counts.print();
        }

        // 执行 flink 程序
        env.execute("stream DataSourceUnion");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                out.collect(new Tuple2<>(token, 1));
            }
        }
    }

}
