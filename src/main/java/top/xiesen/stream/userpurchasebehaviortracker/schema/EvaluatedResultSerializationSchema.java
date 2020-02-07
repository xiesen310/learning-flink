package top.xiesen.stream.userpurchasebehaviortracker.schema;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import top.xiesen.stream.userpurchasebehaviortracker.model.EvaluatedResult;

/**
 * 计算结果序列化类
 */
public class EvaluatedResultSerializationSchema implements KeyedSerializationSchema<EvaluatedResult> {
    @Override
    public byte[] serializeKey(EvaluatedResult element) {
        return element.getUserId().getBytes();
    }

    @Override
    public byte[] serializeValue(EvaluatedResult element) {
        return JSON.toJSONString(element).getBytes();
    }

    @Override
    public String getTargetTopic(EvaluatedResult element) {
        return null;
    }
}
