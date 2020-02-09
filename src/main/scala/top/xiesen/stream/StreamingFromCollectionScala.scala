package top.xiesen.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object StreamingFromCollectionScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = List(10, 15, 20)
    val text = env.fromCollection(data)

    // 针对 map 接收到的数据执行 加 1 操作
    val num = text.map(_ + 1)
    num.print().setParallelism(1)

    env.execute(StreamingFromCollectionScala.getClass.getSimpleName)
  }
}
