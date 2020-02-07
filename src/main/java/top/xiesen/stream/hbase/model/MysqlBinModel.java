package top.xiesen.stream.hbase.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MysqlBinModel {
    public String database;
    public String table;
    public String type;
    public int ts;
    public int xid;
    public Boolean commit;
    public String data;
}
