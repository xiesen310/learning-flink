package top.xiesen.stream.source.custom;

import org.apache.flink.streaming.api.functions.source.SourceFunction;


/**
 * 模拟产生从 1 开始的递增数字
 * SourceFunction 和 SourceContext都需要指定数据类型，如果不指定，代码运行的时候会报错
 *
 * @Description 自定义实现并行度为 1 的 source
 * @className top.xiesen.stream.source.custom.MyNoParallelismSource
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/9 20:12
 */
public class MyNoParallelSource implements SourceFunction<Long> {
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
