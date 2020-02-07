package top.xiesen.stream.userpurchasebehaviortracker.simulator;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * 配置流信息
 */
public class ConfigSimulator {
    public static void main(String[] args) {
        String config = "{\"channel\":\"APP\",\"registerDate\":\"2018-01-01\",\"historyPurchaseTimes\":0,\"maxPurchasePathLength\":3}";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "zorkdata-91:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record;
        String topic = "purchasePathAnalysisConf";

        record = new ProducerRecord<>(topic, null, new Random().nextInt() + "", config);
        producer.send(record);
        producer.close();
    }
}
