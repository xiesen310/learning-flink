package top.xiesen.stream.custompartition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @Description 自定义按照奇偶数进行分区
 * @className top.xiesen.stream.custompartition.MyPartition
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/9 22:04
 */
public class MyPartition implements Partitioner<Long> {
    @Override
    public int partition(Long key, int numPartitions) {
        System.out.println("分区总数: " + numPartitions);
        if (key % 2 == 0) {
            return 0;
        } else {
            return 1;
        }
    }
}
