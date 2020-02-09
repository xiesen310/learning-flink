package top.xiesen.stream.customsource

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingDemoWithMyParallelSourceScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.addSource(new MyParallelSourceScala).setParallelism(2)

    val mapData = text.map(line => {
      println("接收到的数据: " + line)
      line
    })

    val sum = mapData.timeWindowAll(Time.seconds(2)).sum(0)
    sum.print().setParallelism(1)
    env.execute(StreamingDemoWithMyParallelSourceScala.getClass.getSimpleName)
  }
}
