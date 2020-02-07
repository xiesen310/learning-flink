package top.xiesen.sql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import top.xiesen.sql.model.WC;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description WordCountSQL
 * @Author 谢森
 * @Date 2019/8/3 22:31
 */
public class WordCountSQL {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        BatchTableEnvironment tenv = TableEnvironment.getTableEnvironment(env);
        List list = new ArrayList();
        String wordStr = "hello flink hello imooc";
        String[] words = wordStr.split("\\W+");

        for (String word : words) {
            WC wc = new WC(word, 1L);
            list.add(wc);
        }

        DataSet<WC> input = env.fromCollection(list);

        // 注册成 Flink table
        tenv.registerDataSet("wordCount", input, "word, frequency");

//        String sql = "SELECT word,SUM(frequency) as frequency FROM wordCount GROUP BY word";

        // 比较函数(等于/like)
//        String sql = "SELECT word,SUM(frequency) as frequency FROM wordCount where word = 'hello' GROUP BY word";
//        String sql = "SELECT word,SUM(frequency) as frequency FROM wordCount where word LIKE '%i%' GROUP BY word";

        // 算数函数(+/-)
//        String sql = "SELECT word,SUM(frequency) + 1 as frequency FROM wordCount GROUP BY word";
//        String sql = "SELECT word,SUM(frequency) - 1 as frequency FROM wordCount GROUP BY word";

        // 字符串处理函数(UPPER/LOWER/SUBSTRING)
//        String sql = "SELECT UPPER(word) as word,SUM(frequency) as frequency FROM wordCount GROUP BY word";
//        String sql = "SELECT LOWER(word) as word,SUM(frequency) as frequency FROM wordCount GROUP BY word";
//        String sql = "SELECT SUBSTRING(word,2) as word,SUM(frequency) as frequency FROM wordCount GROUP BY word";

        // 其他函数(MD5)
        String sql = "SELECT MD5(word) as word,SUM(frequency) as frequency FROM wordCount GROUP BY word";


        Table table = tenv.sqlQuery(sql);

        DataSet<WC> result = tenv.toDataSet(table, WC.class);
        result.print();



    }
}
