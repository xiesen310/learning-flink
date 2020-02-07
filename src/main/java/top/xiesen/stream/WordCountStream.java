package top.xiesen.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import top.xiesen.batch.WordCountData;

/**
 * 流处理-单词词频统计
 */
public class WordCountStream {
    public static void main(String[] args) throws Exception {
        // 解析命令行参数
        ParameterTool params = ParameterTool.fromArgs(args);

        // 获取执行参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream;
        if (params.has("input")) {
            dataStream = env.readTextFile(params.get("input"));
        } else {
            dataStream = env.fromElements(WordCountData.WORDS);
        }

        // 数据处理
        DataStream<Tuple2<String, Integer>> counts = dataStream.flatMap(new Tokenizer())
                .keyBy(0)
                .sum(1);

        // 输出统计结果
        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
        } else {
            counts.print();
        }

        // 执行 flink 程序
        env.execute("stream wordcount");


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
