package top.xiesen.stream.userpurchasebehaviortracker.model;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

@Data
@ToString
public class Product implements Serializable {
    private static final long serialVersionUID = -410999631338569942L;
    private Integer productId;
    private double price;
    private Integer amount;

}
