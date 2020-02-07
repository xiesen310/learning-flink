package top.xiesen.stream.kakfaconnector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * Flink 消费 kafka 数据
 */
public class StreamingKafkaConsumer {
    public static void main(String[] args) throws Exception {
        // 获取一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "xiesen";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "zorkdata-91:9092");
        prop.setProperty("group.id", "flink");

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), prop);
        DataStreamSource<String> dataSource = env.addSource(myConsumer);
        dataSource.print();
        env.execute("StreamingKafkaConsumer");

    }
}
