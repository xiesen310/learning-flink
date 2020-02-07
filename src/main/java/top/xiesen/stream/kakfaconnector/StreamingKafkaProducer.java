package top.xiesen.stream.kakfaconnector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;

/**
 * Flink kakfa 生产者
 */
public class StreamingKafkaProducer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("192.168.1.91", 9999, "\n");

        String brokerList = "zorkdata-91:9092";
        String topic = "xiesen";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", brokerList);

        FlinkKafkaProducer010<String> kafkaProducer010 = new FlinkKafkaProducer010<>(topic, new SimpleStringSchema(), prop);
        dataStreamSource.addSink(kafkaProducer010);
        env.execute("StreamingKafkaProducer");


    }
}
