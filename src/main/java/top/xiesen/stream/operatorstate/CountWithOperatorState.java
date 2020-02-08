package top.xiesen.stream.operatorstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @className top.xiesen.stream.operatorstate.CountWithOperatorState
 * @Author 谢森
 * @Email xiesen310@163.com
 * @Date 2020/2/8 14:37
 */
public class CountWithOperatorState extends RichFlatMapFunction<Long, Tuple2<Integer, String>> implements CheckpointedFunction {

    /**
     * 托管状态
     */
    private transient ListState<Long> checkPointCountList;

    /**
     * 原始状态
     */
    private List<Long> listBufferElements;

    @Override
    public void flatMap(Long value, Collector<Tuple2<Integer, String>> out) throws Exception {
        if (value == 1) {
            if (listBufferElements.size() > 0) {
                StringBuffer buffer = new StringBuffer();
                for (Long item : listBufferElements) {
                    buffer.append(item + " ");
                }
                out.collect(Tuple2.of(listBufferElements.size(), buffer.toString()));
                listBufferElements.clear();
            }
        } else {
            listBufferElements.add(value);
        }
    }

    /**
     * 做 Checkpoint 的时候对数据做快照
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkPointCountList.clear();
        for (Long item : listBufferElements) {
            checkPointCountList.add(item);
        }
    }

    /**
     * 重复恢复数据
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> listStateDescriptor =
                new ListStateDescriptor<>("checkPointCountList", TypeInformation.of(new TypeHint<Long>() {} ));
        checkPointCountList = context.getKeyedStateStore().getListState(listStateDescriptor);
        if (context.isRestored()) {
            for (Long element : checkPointCountList.get()) {
                listBufferElements.add(element);
            }
        }

    }

    /**
     * 对原始状态初始化
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        listBufferElements = new ArrayList<>();
    }
}
