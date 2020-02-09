package top.xiesen.stream.api

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import top.xiesen.stream.customsource.{MyNoParallelSourceScala}

object StreamingDemoUnionScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text1 = env.addSource(new MyNoParallelSourceScala)
    val text2 = env.addSource(new MyNoParallelSourceScala)
    val unionAll = text1.union(text2)
    val sum = unionAll.map(line => {
      println("接收到的数据: " + line)
      line
    }).timeWindowAll(Time.seconds(2)).sum(0)

    sum.print().setParallelism(1)
    env.execute(StreamingDemoUnionScala.getClass.getSimpleName)
  }
}
