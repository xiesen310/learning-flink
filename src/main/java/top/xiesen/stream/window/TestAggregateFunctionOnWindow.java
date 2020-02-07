package top.xiesen.stream.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description 测试 AggregateFunction
 * @className top.xiesen.stream.window.TestAggregateFunctionOnWindow
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/7 21:45
 */
public class TestAggregateFunctionOnWindow {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取数据
        DataStream<Tuple3<String, String, Integer>> input = env.fromElements(ENGLISH);

        // 求各个班级英语成绩平均分
        DataStream<Double> avgScore = input.keyBy(0)
                .countWindow(2)
                .aggregate(new AvgAggregate());
        avgScore.print();

        env.execute("TestAggregateFunctionOnWindow");
    }

    public static class AvgAggregate implements AggregateFunction<Tuple3<String, String, Integer>, Tuple2<Integer, Integer>, Double> {

        /**
         * 创建累加器保存中间状态(sum count)
         *
         * @return
         */
        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return new Tuple2<>(0, 0);
        }

        /**
         * 将元素添加到累加器并返回新的累加器
         *
         * @param value
         * @param accumulator
         * @return
         */
        @Override
        public Tuple2<Integer, Integer> add(Tuple3<String, String, Integer> value, Tuple2<Integer, Integer> accumulator) {

            return new Tuple2<>(accumulator.f0 + value.f2, accumulator.f1 + 1);
        }

        /**
         * 从累加器提取结果
         *
         * @param accumulator
         * @return
         */
        @Override
        public Double getResult(Tuple2<Integer, Integer> accumulator) {

            return ((double) accumulator.f0) / accumulator.f1;
        }

        /**
         * 累加器合并
         *
         * @param a
         * @param b
         * @return
         */
        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

    public static final Tuple3[] ENGLISH = new Tuple3[]{
            Tuple3.of("class1", "张三", 100),
            Tuple3.of("class1", "李四", 78),
            Tuple3.of("class1", "王五", 99),
            Tuple3.of("class2", "赵六", 81),
            Tuple3.of("class2", "小七", 59),
            Tuple3.of("class2", "小八", 97)
    };
}
