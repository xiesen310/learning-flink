package top.xiesen.stream.api

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import top.xiesen.stream.customsource.MyNoParallelSourceScala

object StreamingDemoConnectScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text1 = env.addSource(new MyNoParallelSourceScala)
    val text2 = env.addSource(new MyNoParallelSourceScala)

    val text2Str = text2.map("str" + _)
    val connectStreams = text1.connect(text2Str)
    val result = connectStreams.map(line1 => {
      line1
    }, line2 => {
      line2
    })

    result.print().setParallelism(1)
    env.execute(StreamingDemoConnectScala.getClass.getSimpleName)
  }
}
