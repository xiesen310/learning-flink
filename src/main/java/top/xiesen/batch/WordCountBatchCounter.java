package top.xiesen.batch;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 统计单词词频
 */
public class WordCountBatchCounter {
    private static final String ACCUMULATOR_NAME = "num-lines";

    public static void main(String[] args) throws Exception {
        // 解析命令行传过来的参数
        ParameterTool params = ParameterTool.fromArgs(args);

        // 获取一个执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取输入数据
        DataSet<String> dataSet;
        if (params.has("input")) {
            dataSet = env.readTextFile(params.get("input"));
        } else {
            dataSet = WordCountData.getDefaultTextLineDataSet(env);
        }

        // 单词词频统计
        DataSet<Tuple2<String, Integer>> counts = dataSet.flatMap(new Tokenizer())
                .groupBy(0)
                .sum(1);

        if (params.has("output")) {
            // 数据输出为 csv 格式
            counts.writeAsCsv(params.get("output"), "\n", " ");

            // 提交执行 flink 应用
            JobExecutionResult jobExecutionResult = env.execute("wordcount example");
            int counter = jobExecutionResult.getAccumulatorResult(ACCUMULATOR_NAME);
            System.out.println("counter=" + counter);
        } else {
            // 数据打印到控制台
            counts.print();
        }


    }

    public static final class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>> {
        // 1. 创建累加器对象
        private IntCounter numLines = new IntCounter();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 2. 注册累加器对象
            getRuntimeContext().addAccumulator(ACCUMULATOR_NAME, this.numLines);

        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            //3. 使用累加器,统计数据行数
            this.numLines.add(1);
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                out.collect(new Tuple2<>(token, 1));
            }
        }
    }
}
