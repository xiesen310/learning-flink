package top.xiesen.stream.userpurchasebehaviortracker.model;

import lombok.Data;
import lombok.ToString;

import java.util.Map;

/**
 * 计算结果
 */
@Data
@ToString
public class EvaluatedResult {
    private String userId;
    private String channel;
    private Integer purchasePathLength;
    private Map<String,Integer> eventTypeCounts;
}
