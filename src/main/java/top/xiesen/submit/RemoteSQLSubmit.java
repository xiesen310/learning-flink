package top.xiesen.submit;

import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.xiesen.submit.sql.SinkDescriptor;

import java.net.URL;
import java.util.*;

public class RemoteSQLSubmit {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteSQLSubmit.class);

    private static void registerSink(Table table, SinkDescriptor sinkDescriptor)
            throws Exception {
        TableSink tableSink = sinkDescriptor.transform(table.getSchema());
        table.writeToSink(tableSink);
    }

    public static void main(String[] args) throws Exception {
        // 集群信息
        Configuration configuration = new Configuration();

        configuration.setString(JobManagerOptions.ADDRESS, "192.168.1.222");
        configuration.setInteger(JobManagerOptions.PORT, 6123);
        configuration.setInteger(RestOptions.PORT, 8081);

        // rest客户端
        RestClusterClient restClient = new RestClusterClient(configuration, "test");
        restClient.setPrintStatusDuringExecution(true);
        restClient.setDetached(true);

        final StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment env = StreamTableEnvironment.getTableEnvironment(execEnv);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "master:9092");
        properties.setProperty("group.id", "test-books");

        // books 的 kafka topic , Flink-kafka
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("books", new SimpleStringSchema(), properties);

        // 从 books 这个 topic 最开始的位置开始消费
        consumer.setStartFromEarliest();

        DataStreamSource<String> topic = execEnv.addSource(consumer);
        SingleOutputStreamOperator<String> kafaSource = topic.map(new MapFunction<String, String>() {
            @Override
            public String map(String book) throws Exception {
                return book;
            }
        });

        // 注册内存表
        env.registerDataStream("books", kafaSource, "name");

        String sql = "SELECT name, count(1) from books group by name";

        Table result = env.sqlQuery(sql);
        env.toRetractStream(result, Row.class).print();

        List<URL> urls = new ArrayList<>();
        ClassLoader usercodeClassLoader = JobWithJars.buildUserCodeClassLoader(urls, Collections.emptyList(), RemoteSQLSubmit.class.getClassLoader());


        StreamGraph streamGraph = execEnv.getStreamGraph();
        streamGraph.setJobName("test");

        JobSubmissionResult submissionResult = restClient.run(streamGraph, urls, Collections.emptyList(), usercodeClassLoader);
        LOGGER.trace(" submit sql request success,jobId:{}", submissionResult.getJobID());

    }
}
