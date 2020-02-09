package top.xiesen.stream.source.custom;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @Description 自定义一个支持并行度的 source
 * RichParallelSourceFunction 会多 open 和 close 方法
 * 针对 source 中如果需要获取其他的资源链接，可以在 open 中获取链接，在 close 中关闭连接
 * @className top.xiesen.stream.source.custom.MyParallelismSource
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/9 20:32
 */
public class MyRichParallelSource extends RichParallelSourceFunction<Long> {
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

    /**
     * 这个方法只会在最开始的时候被调用一次
     * 实现获取链接的方法
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("MyRichParallelSource open ......");
        super.open(parameters);
    }

    /**
     * 实现关闭链接的代码
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        System.out.println("MyRichParallelSource close ......");
        super.close();
    }
}
