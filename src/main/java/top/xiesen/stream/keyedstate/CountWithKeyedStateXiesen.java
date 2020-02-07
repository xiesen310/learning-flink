package top.xiesen.stream.keyedstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class CountWithKeyedStateXiesen extends RichFlatMapFunction<Xiesen, Xiesen> {
    private transient ValueState<Xiesen> sum;

    @Override
    public void flatMap(Xiesen value, Collector<Xiesen> out) throws Exception {
        Xiesen currentSum = sum.value();
        if (null == currentSum) {
            currentSum = new Xiesen(0L, 0L);
        }


        currentSum.setA(currentSum.getA() + 1);
        currentSum.setB(currentSum.getB());
        sum.update(currentSum);
        if (currentSum.getA() >= 3) {
//            out.collect(Tuple2.of(value.f0, currentSum.f1 / currentSum.f0));
            out.collect(new Xiesen(value.getA(), currentSum.getB() / currentSum.getA()));
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Xiesen> descriptor =
                new ValueStateDescriptor<Xiesen>(
                        "avgState",
                        TypeInformation.of(new TypeHint<Xiesen>() {
                        }));
        sum = getRuntimeContext().getState(descriptor);
    }
}
