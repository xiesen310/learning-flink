package top.xiesen.stream.api

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import top.xiesen.stream.customsource.{MyNoParallelSourceScala, StreamingDemoWithMyNoParallelSourceScala}
import org.apache.flink.api.scala._

object StreamingDemoFilterScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.addSource(new MyNoParallelSourceScala)

    val mapData = text.map(line => {
      println("原始接收到的数据: " + line)
      line
    }).filter(_ % 2 == 0)

    val sum = mapData.map(line => {
      println("过滤之后的数据: " + line)
      line
    }).timeWindowAll(Time.seconds(2)).sum(0)
    sum.print().setParallelism(1)
    env.execute(StreamingDemoWithMyNoParallelSourceScala.getClass.getSimpleName)
  }
}
