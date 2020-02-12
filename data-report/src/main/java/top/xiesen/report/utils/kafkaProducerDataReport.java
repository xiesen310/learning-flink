package top.xiesen.report.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Created by xuwei.tech on 2018/11/6.
 */
public class kafkaProducerDataReport {

    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        //指定kafka broker地址
        prop.put("bootstrap.servers", "s103:9092");
        //指定key value的序列化方式
        prop.put("key.serializer", StringSerializer.class.getName());
        prop.put("value.serializer", StringSerializer.class.getName());
        //指定topic名称
        String topic = "auditLog";

        //创建producer链接
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        //{"dt":"2018-01-01 10:11:11","type":"shelf","username":"jack","area":"AREA_US",}

        //生产消息
        while (true) {
            String message = "{\"dt\":\"" + getCurrentTime() + "\",\"type\":\"" + getRandomType() + "\",\"area\":\"" + getRandomArea() + "\"}";
            System.out.println(message);
            producer.send(new ProducerRecord<String, String>(topic, message));
            Thread.sleep(1000);
        }
        //关闭链接
        //producer.close();
    }

    public static String getCurrentTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }

    public static String getRandomArea() {
        String[] types = {"AREA_US", "AREA_TW", "AREA_HK", "AREA_PK", "AREA_KW", "AREA_SA", "AREA_IN"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }


    public static String getRandomType() {
        String[] types = {"shelf", "unshelf", "black", "child_shelf", "child_unshelf"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }


    public static String getRandomUserName() {
        String[] types = {"tom", "toms", "lucy", "allen", "jack"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }


}
