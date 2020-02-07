package top.xiesen.stream.userpurchasebehaviortracker.simulator;

import com.cloudwise.sdg.dic.DicInitializer;
import com.cloudwise.sdg.template.TemplateAnalyzer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * 模拟产生数据
 */
public class UserEventsSimulator {
    public static void main(String[] args) throws Exception {
        //加载词典
        DicInitializer.init();
        // 非购买操作模板
        String userEventTpl = "{\"userId\":\"$Dic{userId}\",\"channel\":\"$Dic{channel}\",\"eventType\":\"$Dic{eventType}\",\"eventTime\":\"$Dic{eventTime}\",\"data\":{\"productId\":$Dic{productId}}}";
        // 购买操作模板
        String purchaseUserEventTpl = "{\"userId\":\"$Dic{userId}\",\"channel\":\"$Dic{channel}\",\"eventType\":\"$Dic{eventType}\",\"eventTime\":\"$Dic{eventTime}\",\"data\":{\"productId\":$Dic{productId},\"price\":$Dic{price},\"amount\":$Dic{amount}}}";

        //创建模版分析器
        TemplateAnalyzer userEventTplAnalyzer = new TemplateAnalyzer("userEvent", userEventTpl);
        TemplateAnalyzer purchaseUserEventTplAnalyzer = new TemplateAnalyzer("purchaseUserEvent", purchaseUserEventTpl);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "zorkdata-91:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record;
        String topic = "purchasePathAnalysisInPut";

        for (int i = 1; i < 100000; i++) {
            record = new ProducerRecord<>(topic, null, new Random().nextInt() + "", userEventTplAnalyzer.analyse());
            producer.send(record);

            long sleep = (long) (Math.random() * 2000);
            Thread.sleep(sleep);

            if (sleep % 2 == 0 && sleep > 800) {
                record = new ProducerRecord<>(topic, null, new Random().nextInt() + "", purchaseUserEventTplAnalyzer.analyse());
                producer.send(record);
            }
        }

        //分析模版生成模拟数据
        /*String userEventAnalyse = userEventTplAnalyzer.analyse();
        String purchaseUserEventAnalyse = purchaseUserEventTplAnalyzer.analyse();
        System.out.println(userEventAnalyse);
        System.out.println(purchaseUserEventAnalyse);*/
    }
}
