package top.xiesen.stream.source.custom;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * @Description 自定义一个支持并行度的 source
 * @className top.xiesen.stream.source.custom.MyParallelismSource
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/9 20:32
 */
public class MyParallelSource implements ParallelSourceFunction<Long> {
    private boolean isRunning = true;
    private long count = 1L;

    /**
     * 主要的方法
     * 启动一个 source
     * 大部分情况下，都需要 run 方法实现循环，这样就可以循环产生数据了
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(count);
            count++;
            // 每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    /**
     * 取消一个 cancel 的时候会调用方法
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}
