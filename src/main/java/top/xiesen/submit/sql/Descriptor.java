package top.xiesen.submit.sql;

public interface Descriptor {

    String type();

    void validate() throws Exception;

}
