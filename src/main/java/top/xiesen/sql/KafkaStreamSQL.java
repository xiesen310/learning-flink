package top.xiesen.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @Description KafkaStreamSQL
 * @Author 谢森
 * @Date 2019/8/4 21:17
 */
public class KafkaStreamSQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = TableEnvironment.getTableEnvironment(senv);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "master:9092");
        properties.setProperty("group.id", "test-books");

        // books 的 kafka topic , Flink-kafka
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("books", new SimpleStringSchema(), properties);

        // 从 books 这个 topic 最开始的位置开始消费
        consumer.setStartFromEarliest();

        DataStreamSource<String> topic = senv.addSource(consumer);
        SingleOutputStreamOperator<String> kafaSource = topic.map(new MapFunction<String, String>() {
            @Override
            public String map(String book) throws Exception {
                return book;
            }
        });

        // 注册内存表
        tenv.registerDataStream("books", kafaSource, "name");

        String sql = "SELECT name, count(1) from books group by name";

        Table result = tenv.sqlQuery(sql);

        // 非常重要的知识点: 回退更新 java：1 --> java: 2
        tenv.toRetractStream(result, Row.class).print();

        // 提交
        senv.execute();
    }
}
