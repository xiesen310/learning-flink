package top.xiesen.stream.userpurchasebehaviortracker.model;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * 用户实体类
 */
@Data
@ToString
public class UserEvent implements Serializable {
    private static final long serialVersionUID = 573106100150847404L;

    private String userId;
    private String channel;
    private String eventType;
    private long eventTime;
    private Product data;
}
