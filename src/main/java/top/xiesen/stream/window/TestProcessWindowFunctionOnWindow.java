package top.xiesen.stream.window;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @className top.xiesen.stream.window.TestProcessWindowFunctionOnWindow
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/7 22:09
 */
public class TestProcessWindowFunctionOnWindow {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取数据
        DataStream<Tuple3<String, String, Integer>> input = env.fromElements(ENGLISH);
        // 求各个班级英语成绩平均分
        DataStream<Double> avgScore = input.keyBy(0)
                .countWindow(2)
                .process(new MyProcessWindowFunction());
        avgScore.print();

        env.execute("TestProcessWindowFunctionOnWindow");
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple3<String, String, Integer>, Double, Tuple, GlobalWindow> {

        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple3<String, String, Integer>> elements, Collector<Double> out) throws Exception {
            Integer sum = 0;
            Integer count = 0;
            for (Tuple3<String, String, Integer> in : elements) {
                sum += in.f2;
                count++;
            }
            out.collect((double) (sum / count));
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
