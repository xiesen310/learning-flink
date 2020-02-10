package top.xiesen.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Description word count
 * @className top.xiesen.stream.SocketDemoWordCountCheckpoint
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/10 13:46
 */
public class SocketDemoWordCountCheckpoint {
    public static void main(String[] args) throws Exception {
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.out.println("No port set. use default port 9000");
            port = 9000;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每 1000ms 进行启动一个检查点
        env.enableCheckpointing(1000L);
        // 设置模式为 EXACTLY_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间至少有 500ms 的间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);
        // 检查点必须在 1 分钟之内完成，或者被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许进行一个 检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦 flink 程序 cancel 后，会保留 checkpoint 数据，以便根据实际情况恢复到指定的 Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        // 设置 statebacked
//        env.setStateBackend(new MemoryStateBackend());
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop100:9000/flink/checkpoints"));
//        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop100:9000/flink/checkpoints"));

        String hostname = "192.168.0.106";
        String delimiter = "\n";
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);

        DataStream<Tuple2<Integer, Integer>> intData = text.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String value) throws Exception {
                return new Tuple2<>(1, Integer.parseInt(value));
            }
        });

        intData.keyBy(0)
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<Integer, Integer>, Object, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple key, Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<Object> out) throws Exception {
                        System.out.println("执行 process....");
                        long count = 0L;
                        for (Tuple2<Integer, Integer> element : elements) {
                            count++;
                        }
                        out.collect("window: " + context.window() + ",count:" + count);
                    }
                }).print();

        env.execute(SocketDemoWordCountCheckpoint.class.getSimpleName());
    }
}
