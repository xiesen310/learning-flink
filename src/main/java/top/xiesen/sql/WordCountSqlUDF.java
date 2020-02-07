package top.xiesen.sql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import top.xiesen.sql.model.WC;

import java.util.ArrayList;

/**
 * @Description 自定义UDF
 * @Author 谢森
 * @Date 2019/8/4 19:46
 */
public class WordCountSqlUDF {
    /**
     * 用户自定义函数 UDF、UDAF、UDTF
     * UDF: 是普通的标量函数，接收0个，1个，多个输入，返回一个输出,例如 接收一个字符串，返回它的长度
     * UDAF: 聚合函数，接收多条数据，返回一个汇总值，例如 统计某一段时间内商品的销量
     * UDTF: 表值函数，接收0个，1个，多个输入,返回多个参数
     * <p>
     * 自定义函数步骤
     * 1. 继承函数ScalarFunction
     * 2. 覆写方法eval
     * 3. 注册函数
     * 4. 应用
     */
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tenv = TableEnvironment.getTableEnvironment(env);

        // 注册自定义函数
        tenv.registerFunction("StringToSite",new StringToSite(".cn"));




        // hello.com flink.com
        String words = "hello flink hello imooc";
        String[] split = words.split("\\W+");

        ArrayList<WC> list = new ArrayList<>();

        for (String word : split) {
            WC wc = new WC(word, 1L);
            list.add(wc);
        }

        DataSet<WC> input = env.fromCollection(list);
        tenv.registerDataSet("wordCount", input, "word,frequency");
        String sql = "SELECT StringToSite(word) as word, SUM(frequency) as frequency from wordCount GROUP BY word";
        Table table = tenv.sqlQuery(sql);

        DataSet<WC> result = tenv.toDataSet(table, WC.class);
        result.print();
    }


    /**
     * 自定义函数 StringToSite  hello --> hello.com
     */
    public static class StringToSite extends ScalarFunction {
        private String address;

        public StringToSite(String address) {
            this.address = address;
        }

        public String eval(String s) {
            return s + address;
        }
    }
}
