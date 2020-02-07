package top.xiesen.sql.model;

/**
 * @Description PlayerData
 * @Author 谢森
 * @Date 2019/8/4 20:35
 */
public class PlayerData {
    /**
     *赛季,球员,出场,助攻,抢断,盖帽,得分
     */

    private String season;
    private String player;
    private Integer first_court;
    private Double assists;
    private Double steals;
    private Double blocks;
    private Double scores;

    public PlayerData() {
    }

    public PlayerData(String season, String player, Integer first_court, Double assists, Double steals, Double blocks, Double scores) {
        this.season = season;
        this.player = player;
        this.first_court = first_court;
        this.assists = assists;
        this.steals = steals;
        this.blocks = blocks;
        this.scores = scores;
    }

    public String getSeason() {
        return season;
    }

    public void setSeason(String season) {
        this.season = season;
    }

    public String getPlayer() {
        return player;
    }

    public void setPlayer(String player) {
        this.player = player;
    }

    public Integer getFirst_court() {
        return first_court;
    }

    public void setFirst_court(Integer first_court) {
        this.first_court = first_court;
    }

    public Double getAssists() {
        return assists;
    }

    public void setAssists(Double assists) {
        this.assists = assists;
    }

    public Double getSteals() {
        return steals;
    }

    public void setSteals(Double steals) {
        this.steals = steals;
    }

    public Double getBlocks() {
        return blocks;
    }

    public void setBlocks(Double blocks) {
        this.blocks = blocks;
    }

    public Double getScores() {
        return scores;
    }

    public void setScores(Double scores) {
        this.scores = scores;
    }

    @Override
    public String toString() {
        return "{" +
                "season='" + season + '\'' +
                ", player='" + player + '\'' +
                ", first_court=" + first_court +
                ", assists=" + assists +
                ", steals=" + steals +
                ", blocks=" + blocks +
                ", scores=" + scores +
                '}';
    }
}
