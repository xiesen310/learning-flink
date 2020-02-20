package top.xiesen.kafka.test;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * @Description KafkaTest
 * @className top.xiesen.kafka.test.KafkaTest
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/20 11:05
 */
public class KafkaTest {
    public static void main(String[] args) throws Exception {
        Properties sourceProp = new Properties();
        sourceProp.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "s103:9092");
        sourceProp.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-test");
        sourceProp.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        sourceProp.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        sourceProp.setProperty("zookeeper.connect", "s103:2181/kafka08");
        sourceProp.setProperty(FlinkKafkaConsumer08.GET_PARTITIONS_RETRIES_KEY, "1");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        FlinkKafkaConsumer08<String> kafkaConsumer08 = new FlinkKafkaConsumer08<>("test1",
                new SimpleStringSchema(), sourceProp);

        DataStream<String> kafkaSource = env.addSource(kafkaConsumer08);

        kafkaSource.print();

        env.execute(KafkaTest.class.getSimpleName());

    }
}
