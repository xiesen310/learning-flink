package top.xiesen.stream.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Description 二次开发
 * @className top.xiesen.stream.sink.MyRedisSink
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/7 19:42
 */
public class MyRedisSink {
    public static void main(String[] args) throws Exception {
        // 获取执行参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("192.168.0.106", 9999, "\n");

        DataStream<Tuple2<String, String>> redisWordsData = dataStream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<>("redis_words", value);
            }
        });

        FlinkJedisPoolConfig build =
                new FlinkJedisPoolConfig.Builder().setHost("192.168.0.106").setPort(6379).build();

        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(build, new MyRedisMapper());

        redisWordsData.addSink(redisSink);

        env.execute("MyRedisSink");
    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }
}
