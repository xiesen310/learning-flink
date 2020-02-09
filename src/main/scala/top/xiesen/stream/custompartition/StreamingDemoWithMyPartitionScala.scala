package top.xiesen.stream.custompartition

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import top.xiesen.stream.customsource.MyNoParallelSourceScala

object StreamingDemoWithMyPartitionScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val text1 = env.addSource(new MyNoParallelSourceScala)

    // 把 long 类型的数据 转换成 tuple 类型
    val tupleData = text1.map(line => {
      Tuple1(line) // 注意 Tuple1 实现方式
    })

    val partitionData = tupleData.partitionCustom(new MyPartitionScala, 0)

    val result = partitionData.map(line => {
      println("当前线程 id: " + Thread.currentThread().getId + " ,value:" + line)
      line._1
    })

    result.print().setParallelism(1)
    env.execute(StreamingDemoWithMyPartitionScala.getClass.getSimpleName)
  }

}
