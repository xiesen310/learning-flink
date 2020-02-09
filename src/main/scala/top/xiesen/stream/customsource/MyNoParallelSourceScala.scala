package top.xiesen.stream.customsource

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
 * 创建自定义并行度为1 的 source
 * 实现从1 开始产生递增数字
 */
class MyNoParallelSourceScala extends SourceFunction[Long] {
  var count = 1L
  var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
