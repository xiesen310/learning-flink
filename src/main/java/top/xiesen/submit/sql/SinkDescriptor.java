package top.xiesen.submit.sql;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.FormatDescriptor;

import java.util.List;

public abstract class SinkDescriptor implements CoreDescriptor<TableSchema> {

    private String name;

    private List<Field> schema;

    private FormatDescriptor format;

    public SinkDescriptor(String name, List<Field> schema, FormatDescriptor format) {
        this.name = name;
        this.schema = schema;
        this.format = format;
    }

    public SinkDescriptor(String name, List<Field> schema) {
        this.name = name;
        this.schema = schema;
    }


/*
     public static SinkDescriptor from(Sink sink) throws Exception {
        SinkDescriptor descriptor = DescriptorFactory.me.find(sink.getType().toString().toLowerCase(), SinkDescriptor.class);
        if (descriptor == null) {
            throw new UnsupportedOperationException("Unknow sink type:" + sink.getType());
        }
        SinkDescriptor sinkDescriptor = BindPropertiesUtil.bindProperties(sink.getConfig(), descriptor.getClass());
        return sinkDescriptor;
    }
*/

/*    public TableSchema createTableSchema(){
        if(schema == null || schema.size() == 0){
            return null;
        }
        String[] columnNames = new String[schema.size()];
        TypeInformation[] columnTypes = new TypeInformation[schema.size()];
        for (int i = 0; i < schema.size(); i++) {
            columnNames[i] = schema.get(i).getName();
            TypeInformation typeInformation = TypeUtils.readTypeInfo(schema.get(i).getType());
            columnTypes[i] = typeInformation;
        }
        return new TableSchema(columnNames, columnTypes);
    }*/
/*

    public SerializationSchema createSerializationSchema(TypeInformation typeInformation) throws Exception {
        if(format == null){
            return null;
        }
        return format.transform(new Tuple2<>(typeInformation, false));
    }
*/


    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Field> getSchema() {
        return schema;
    }

    public void setSchema(List<Field> schema) {
        this.schema = schema;
    }

    public FormatDescriptor getFormat() {
        return format;
    }

    public void setFormat(FormatDescriptor format) {
        this.format = format;
    }
}