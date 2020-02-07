package top.xiesen.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import top.xiesen.sql.model.PlayerData;
import top.xiesen.sql.model.Result;

/**
 * @Description TableSql
 * @Author 谢森
 * @Date 2019/8/4 20:42
 */
public class TableSql {
    public static void main(String[] args) throws Exception {
        // 1. 获取上下文环境 table 环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tenv = BatchTableEnvironment.getTableEnvironment(env);

        // 2. 读取 score.csv 文件
        DataSet<String> input = env.readTextFile("D:\\Develop\\workspace\\flink\\learning-flink\\doc\\score.csv");
        input.print();
        DataSet<PlayerData> topInput = input.map(new MapFunction<String, PlayerData>() {
            @Override
            public PlayerData map(String s) throws Exception {
                String[] split = s.split(",");

                return new PlayerData(
                        String.valueOf(split[0]),
                        String.valueOf(split[1]),
                        Integer.valueOf(split[2]),
                        Double.valueOf(split[3]),
                        Double.valueOf(split[4]),
                        Double.valueOf(split[5]),
                        Double.valueOf(split[6])
                );
            }
        });

        // 3. 注册内存表
        Table topScore = tenv.fromDataSet(topInput);
        tenv.registerTable("score",topScore);

        // 4. 编写 sql  然后提交执行
        String sql = "SELECT player,COUNT(season) as num from score group by player order by num desc";
        Table queryResult = tenv.sqlQuery(sql);

        // 5. 结果进行打印
        DataSet<Result> result = tenv.toDataSet(queryResult, Result.class);
        result.print();
    }
}
