package top.xiesen.clean;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;
import top.xiesen.clean.source.MyRedisSource;

import java.util.HashMap;
import java.util.Properties;

/**
 * bin/kafka-topics.sh --zookeeper s103:2181/kafka011 --list
 * bin/kafka-topics.sh --zookeeper s103:2181/kafka011 --create --partitions 1 --replication-factor 1 --topic allData
 * bin/kafka-topics.sh --zookeeper s103:2181/kafka011 --create --partitions 1 --replication-factor 1 --topic allDataClean
 *
 * @Description 数据清洗需求
 * 组装代码
 * @className top.xiesen.clean.DataClean
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/12 10:09
 */
public class DataClean {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 指定 kafka source
        String topic = "allData";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "s103:9092");
        prop.setProperty("group.id", "flink-allData");

        // 获取 kafka 中的数据
        // {"dt":"2020-02-12 11:11:11","countryCode":"US","data":[{"type:"s1","score"：“0.3”，“level”:"A"},{"type:"s1","score"：“0.3”，“level”:"A"}]}
        FlinkKafkaConsumer011<String> kafkaSource = new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(), prop);
        DataStreamSource<String> data = env.addSource(kafkaSource);

        // 最新的国家码和大区的映射关系
        DataStream<HashMap<String, String>> mapData = env.addSource(new MyRedisSource()).broadcast();

        DataStream<String> resData = data.connect(mapData).flatMap(new CoFlatMapFunction<String, HashMap<String, String>, String>() {
            // 存储国家和大区的映射关系
            HashMap<String, String> allMap = new HashMap<>();

            // flatMap1 处理的是 kafka 中的数据
            @Override
            public void flatMap1(String value, Collector<String> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String dt = jsonObject.getString("dt");
                String countryCode = jsonObject.getString("countryCode");

                // 获取大区
                String area = allMap.get(countryCode);

                JSONArray jsonArray = jsonObject.getJSONArray("data");
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jsonObject1 = jsonArray.getJSONObject(i);
                    jsonObject1.put("area", area);
                    jsonObject1.put("dt", dt);
                    out.collect(jsonObject1.toJSONString());
                }
            }

            // flatMap2 处理的是 redis 中的数据
            @Override
            public void flatMap2(HashMap<String, String> value, Collector<String> out) throws Exception {
                this.allMap = value;
            }
        });

        String outTopic = "allDataClean";
        Properties outProp = new Properties();
        outProp.setProperty("bootstrap.servers", "s103:9092");
        // 第一种方案, 设置 FlinkKafkaProducer011 里面事务超时时间
        // 设置事务超时时间
        outProp.setProperty("transaction.timeout.ms", 60000 * 15 + "");

        // 第二种方案, 设置 kafka的最大事务超时时间
        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(outTopic,
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
                outProp,
                FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);

        resData.addSink(myProducer);

        env.execute(DataClean.class.getSimpleName());
    }
}
