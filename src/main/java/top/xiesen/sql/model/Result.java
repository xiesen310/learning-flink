package top.xiesen.sql.model;

/**
 * @Description Result
 * @Author 谢森
 * @Date 2019/8/4 20:41
 */
public class Result {
    private String player;
    private Long num;

    public Result() {
    }

    public Result(String player, Long num) {
        this.player = player;
        this.num = num;
    }

    public String getPlayer() {
        return player;
    }

    public void setPlayer(String player) {
        this.player = player;
    }

    public Long getNum() {
        return num;
    }

    public void setNum(Long num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return "{" +
                "player='" + player + '\'' +
                ", num=" + num +
                '}';
    }
}
