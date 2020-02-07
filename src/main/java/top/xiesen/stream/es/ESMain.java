package top.xiesen.stream.es;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import top.xiesen.batch.WordCountData;
import top.xiesen.stream.WordCountStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * es main
 */
@Slf4j
public class ESMain {

    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);
        // 获取执行参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> dataStream;
        if (params.has("input")) {
            dataStream = env.readTextFile(params.get("input"));
        } else {
            dataStream = env.fromElements(WordCountData.WORDS);
        }

        // 数据处理
        DataStream<Tuple2<String, Integer>> counts = dataStream.flatMap(new WordCountStream.Tokenizer())
                .keyBy(0)
                .sum(1);


        List<HttpHost> httpHosts = new ArrayList<>();

        httpHosts.add(new HttpHost("zorkdata-95",9200));

        counts.addSink(new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void process(Tuple2<String, Integer> t, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {

                Map<String, String> bigMap = new HashMap<>();
                bigMap.put("test", "aaa");
                requestIndexer.add(Requests.indexRequest().index("esdemo").type("_doc").source(bigMap, XContentType.JSON));
            }
        }).build());

        try {
            env.execute("flink learning connectors es6");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
