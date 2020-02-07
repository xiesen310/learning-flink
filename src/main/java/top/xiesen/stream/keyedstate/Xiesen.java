package top.xiesen.stream.keyedstate;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Xiesen {
    private long a;
    private long b;

    public Xiesen() {
    }

    public Xiesen(long a, long b) {
        this.a = a;
        this.b = b;
    }
}
