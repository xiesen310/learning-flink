package top.xiesen.submit.sql;

import org.apache.flink.table.descriptors.Descriptor;

public interface CoreDescriptor<R> extends Descriptor {

    String getName();

    <T> T transform() throws Exception;

    default <T> T transform(R param) throws Exception {
        return transform();
    }

}
