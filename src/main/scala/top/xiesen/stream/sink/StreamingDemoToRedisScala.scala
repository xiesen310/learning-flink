package top.xiesen.stream.sink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object StreamingDemoToRedisScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("192.168.0.106", 9999, '\n')

    // 对数据进行封装 例如: lpush  l_words word
    val tupleData = text.map(line => {
      Tuple2("l_words_scala", line)
    })

    val config = new FlinkJedisPoolConfig.Builder().setHost("192.168.0.106").setPort(6379).build()
    val redisSink = new RedisSink[Tuple2[String, String]](config, new MyRedisMapperScala())
    tupleData.addSink(redisSink)

    env.execute(StreamingDemoToRedisScala.getClass.getSimpleName)
  }

  class MyRedisMapperScala extends RedisMapper[Tuple2[String, String]] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.LPUSH)
    }

    override def getKeyFromData(t: (String, String)): String = {
      t._1
    }

    override def getValueFromData(t: (String, String)): String = {
      t._2
    }
  }

}
