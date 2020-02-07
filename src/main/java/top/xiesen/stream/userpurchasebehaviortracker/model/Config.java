package top.xiesen.stream.userpurchasebehaviortracker.model;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

@Data
@ToString
public class Config implements Serializable {
    private static final long serialVersionUID = -3194546555431562289L;
    private String channel;
    private String registerDate;
    private Integer historyPurchaseTimes;
    private Integer maxPurchasePathLength;

    public Config() {
    }

    public Config(String channel, String registerDate, Integer historyPurchaseTimes, Integer maxPurchasePathLength) {
        this.channel = channel;
        this.registerDate = registerDate;
        this.historyPurchaseTimes = historyPurchaseTimes;
        this.maxPurchasePathLength = maxPurchasePathLength;
    }
}
