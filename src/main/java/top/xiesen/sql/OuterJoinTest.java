package top.xiesen.sql;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;

/**
 * @Description OuterJoinTest
 * @Author 谢森
 * @Date 2019/8/4 19:06
 */
public class OuterJoinTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // tuple2<用户id，用户姓名>
        ArrayList<Tuple2<Integer,String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1,"小李"));
        data1.add(new Tuple2<>(2,"小王"));
        data1.add(new Tuple2<>(3,"小张"));

        // tuple2<用户id，所在城市>
        ArrayList<Tuple2<Integer,String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1,"北京"));
        data2.add(new Tuple2<>(2,"上海"));
        data2.add(new Tuple2<>(4,"广州"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);

        /**
         * 左外连接
         * 注意: second 这个tuple中的元素可能为null
         */
        text1.leftOuterJoin(text2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (second == null) {
                            return new Tuple3<>(first.f0,first.f1,"null");
                        }else {
                            return new Tuple3<>(first.f0,first.f1,second.f1);
                        }
                    }
                }).print();

        /**
         * 右外连接
         * 注意: first这个 tuple中的元素可能为null
         */

    }
}
