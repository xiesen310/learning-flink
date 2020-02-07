package top.xiesen.stream.userpurchasebehaviortracker.start;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import top.xiesen.stream.userpurchasebehaviortracker.function.ConnectesdBroadcastProcessFunction;
import top.xiesen.stream.userpurchasebehaviortracker.model.Config;
import top.xiesen.stream.userpurchasebehaviortracker.model.EvaluatedResult;
import top.xiesen.stream.userpurchasebehaviortracker.model.UserEvent;
import top.xiesen.stream.userpurchasebehaviortracker.schema.ConfigDeserializationSchema;
import top.xiesen.stream.userpurchasebehaviortracker.schema.EvaluatedResultSerializationSchema;
import top.xiesen.stream.userpurchasebehaviortracker.schema.UserEventDeserializationSchema;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 主程序入口
 */
public class Launcher {
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String GROUP_ID = "group.id";
    public static final String INPUT_EVENT_TOPIC = "input-event-topic";
    public static final String INPUT_CONFIG_TOPIC = "input-config-topic";
    public static final String OUTPUT_TOPIC = "output-topic";
    public static final String RETRIES = "retries";


    /**
     * keyed state 数据结构
     */
    public static final MapStateDescriptor<String, Config> configStateDescriptor =
            new MapStateDescriptor<String, Config>("configBroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<Config>() {
                    }));

    public static void main(String[] args) throws Exception {
        // 获取执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 检查输入参数
        ParameterTool params = parameterCheck(args);

        // 设置 time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /**
         * chekpoint
         */
        // 启动checkpoint
        env.enableCheckpointing(600000L);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 语义保证
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoint 最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(30000L);
        // checkpoint 超时时间
        checkpointConfig.setCheckpointTimeout(10000L);
        // 启动外部持久化检查点
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /**
         * stateBackend
         */

//        env.setStateBackend(new FsStateBackend("hdfs://zorkdata-151:8020/flink/checkpoints/customer-purchase-behavior-traker"));

        /**
         * restart 策略
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(30, TimeUnit.SECONDS)));

        /**
         * kafka consumer
         */

        Properties consumerProp = new Properties();
        consumerProp.setProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS));
        consumerProp.setProperty(GROUP_ID, params.get(GROUP_ID));

        /**
         * 读取 kakfa 事件流
         */
        FlinkKafkaConsumer010<UserEvent> kafkaUserEventSource = new FlinkKafkaConsumer010<UserEvent>(
                params.get(INPUT_CONFIG_TOPIC),
                new UserEventDeserializationSchema(),
                consumerProp);

        KeyedStream<UserEvent, String> customerUserEventStream = env.addSource(kafkaUserEventSource)
                .assignTimestampsAndWatermarks(new CustomWatermarkExtractor(org.apache.flink.streaming.api.windowing.time.Time.hours(24)))
                .keyBy(new KeySelector<UserEvent, String>() {
                    @Override
                    public String getKey(UserEvent userEvent) throws Exception {
                        return userEvent.getUserId();
                    }
                });

        customerUserEventStream.print();

        /**
         * 读取 kakfa 配置流信息
         */
        FlinkKafkaConsumer010<Config> kafkaConfigSource = new FlinkKafkaConsumer010<Config>(
                params.get(INPUT_CONFIG_TOPIC),
                new ConfigDeserializationSchema(),
                consumerProp);

        final BroadcastStream<Config> configBroadcastStream = env.addSource(kafkaConfigSource)
                .broadcast(configStateDescriptor);

        /**
         * 连接事件流和配置流
         */
        DataStream<EvaluatedResult> connectStream = customerUserEventStream
                .connect(configBroadcastStream)
                .process(new ConnectesdBroadcastProcessFunction());

        Properties producerProp = new Properties();
        producerProp.setProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS));
        producerProp.setProperty(RETRIES, "3");
        FlinkKafkaProducer010<EvaluatedResult> kafkaProducer = new FlinkKafkaProducer010<>(
                params.get(OUTPUT_TOPIC),
                new EvaluatedResultSerializationSchema(),
                producerProp);
        /**
         * at_least_once 配置
         */
        kafkaProducer.setLogFailuresOnly(false);
        kafkaProducer.setFlushOnCheckpoint(true);
        connectStream.addSink(kafkaProducer);

        env.execute("UserPurchaseBehaviorTracker");

    }


    /**
     * 参数校验
     * --bootstrap.servers zorkdata-91:9092 --group.id tets
     * --input-event-topic purchasePathAnalysisInPut
     * --input-config-topic purchasePathAnalysisConf
     * --output-topic purchasePathAnalysisOutPut
     *
     * @param args
     * @return
     */
    @SuppressWarnings("all")
    public static ParameterTool parameterCheck(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        if (!params.has(BOOTSTRAP_SERVERS)) {
            System.err.println("---------------- paramter[" + BOOTSTRAP_SERVERS + "] is required ----------------");
            System.exit(-1);
        }

        if (!params.has(GROUP_ID)) {
            System.err.println("---------------- paramter[" + GROUP_ID + "] is required ----------------");
            System.exit(-1);
        }

        if (!params.has(INPUT_EVENT_TOPIC)) {
            System.err.println("---------------- paramter[" + INPUT_EVENT_TOPIC + "] is required ----------------");
            System.exit(-1);
        }

        if (!params.has(INPUT_CONFIG_TOPIC)) {
            System.err.println("---------------- paramter[" + INPUT_CONFIG_TOPIC + "] is required ----------------");
            System.exit(-1);
        }

        if (!params.has(OUTPUT_TOPIC)) {
            System.err.println("---------------- paramter[" + OUTPUT_TOPIC + "] is required ----------------");
            System.exit(-1);
        }
        return params;
    }

    /**
     * 自定义 watermark
     */

    private static class CustomWatermarkExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserEvent> {

        public CustomWatermarkExtractor(org.apache.flink.streaming.api.windowing.time.Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(UserEvent userEvent) {
            return userEvent.getEventTime();
        }
    }
}
