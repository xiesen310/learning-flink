package top.xiesen.stream.keyedstate;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestKeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Xiesen> inputStream = env.fromElements(
                new Xiesen(1L, 4l),
                new Xiesen(2L, 3L),
                new Xiesen(3L, 1L),
                new Xiesen(1L, 2L),
                new Xiesen(3L, 2L),
                new Xiesen(1L, 2L),
                new Xiesen(2L, 2L),
                new Xiesen(2L, 9L)
        );
        /*DataStreamSource<Tuple2<Long, Long>> inputStream = env.fromElements(
                Tuple2.of(1L, 4l),
                Tuple2.of(2L, 3L),
                Tuple2.of(3L, 1L),
                Tuple2.of(1L, 2L),
                Tuple2.of(3L, 2L),
                Tuple2.of(1L, 2L),
                Tuple2.of(2L, 2L),
                Tuple2.of(2L, 9L)
        );*/

        inputStream.keyBy(Xiesen::getA)
//                .flatMap(new CountWithKeyedState())
                .flatMap(new CountWithKeyedStateXiesen())
                .setParallelism(10)
                .print();
        env.execute("TestKeyedState");

    }
}
