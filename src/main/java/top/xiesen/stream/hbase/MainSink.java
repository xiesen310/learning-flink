package top.xiesen.stream.hbase;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import top.xiesen.stream.hbase.model.MysqlBinModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
public class MainSink {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(1000);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");


        SingleOutputStreamOperator<MysqlBinModel> student = env.addSource(new FlinkKafkaConsumer010<>(
                "test",   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(9)
                .map(string -> JSON.parseObject(string, MysqlBinModel.class)).setParallelism(9);
//        student.print();
        long start =System.currentTimeMillis();

        student.timeWindowAll(Time.seconds(3)).apply(new AllWindowFunction<MysqlBinModel, List<MysqlBinModel>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<MysqlBinModel> values, Collector<List<MysqlBinModel>> out) throws Exception {
                ArrayList<MysqlBinModel> students = Lists.newArrayList(values);
                if (students.size() > 0) {
                    System.out.println("1s内收集到 mysql表 的数据条数是：" + students.size());
                    long end =System.currentTimeMillis();
                    System.out.printf("已经用时time:%d\n",end-start);
                    out.collect(students);
                }
            }
        }).addSink(new SinkToHBase()).setParallelism(9);

        env.execute("flink learning connectors HBase");
    }
}
