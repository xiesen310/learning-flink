package top.xiesen.stream.api

import java.{lang, util}

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import top.xiesen.stream.customsource.MyNoParallelSourceScala

object StreamingDemoSplitScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.addSource(new MyNoParallelSourceScala)

    val splitStream = text.split(new OutputSelector[Long] {
      override def select(value: Long): lang.Iterable[String] = {
        val list = new util.ArrayList[String]()
        if (value % 2 == 0) {
          list.add("even")
        } else {
          list.add("odd")
        }
        list
      }
    })
    val evenStream = splitStream.select("even")
    evenStream.print().setParallelism(1)
    env.execute(StreamingDemoSplitScala.getClass.getSimpleName)
  }
}
