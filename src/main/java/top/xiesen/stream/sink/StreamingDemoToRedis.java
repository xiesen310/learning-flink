package top.xiesen.stream.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Description 接收 socket 数据，把数据保存到 redis list 中
 * @className top.xiesen.stream.sink.StreamingDemoToRedis
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/9 22:23
 */
public class StreamingDemoToRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("192.168.0.106", 9999, "\n");

        // 对数据进行封装 例如: lpush  l_words word
        DataStream<Tuple2<String, String>> tupleData = text.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<>("l_words", value);
            }
        });

        // 创建 redis 配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("192.168.0.106").setPort(6379).build();

        // 创建 redis sink
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(config, new MyRedisMapper());

        tupleData.addSink(redisSink);

        String jobName = StreamingDemoToRedis.class.getSimpleName();
        env.execute(jobName);
    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        /**
         * 从接收到的数据中获取 key
         *
         * @param data
         * @return
         */
        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        /**
         * 从接收到的数据中获取 value
         *
         * @param data
         * @return
         */
        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }

}
