package top.xiesen.submit.sql;

public class TimeAttribute {

    private Timestamps timestamps;

    private Watermarks watermarks;

    public Timestamps getTimestamps() {

        return timestamps;
    }

    public void setTimestamps(Timestamps timestamps) {
        this.timestamps = timestamps;
    }

    public Watermarks getWatermarks() {
        return watermarks;
    }

    public void setWatermarks(Watermarks watermarks) {
        this.watermarks = watermarks;
    }
}
