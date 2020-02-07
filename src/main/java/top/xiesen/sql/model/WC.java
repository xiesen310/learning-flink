package top.xiesen.sql.model;

/**
 * @Description wordcount 实体类
 * @Author 谢森
 * @Date 2019/8/4 17:42
 */
public class WC {
    private String word;
    private Long frequency;

    public WC() {
    }

    public WC(String word, Long frequency) {
        this.word = word;
        this.frequency = frequency;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Long getFrequency() {
        return frequency;
    }

    public void setFrequency(Long frequency) {
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return "{" +
                "word='" + word + '\'' +
                ", frequency=" + frequency +
                '}';
    }
}
