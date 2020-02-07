package top.xiesen.stream.userpurchasebehaviortracker.function;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import top.xiesen.stream.userpurchasebehaviortracker.model.*;
import top.xiesen.stream.userpurchasebehaviortracker.start.Launcher;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 处理函数
 */
public class ConnectesdBroadcastProcessFunction extends KeyedBroadcastProcessFunction<String, UserEvent, Config, EvaluatedResult> {
    /**
     * 默认配置
     */
    private Config defaultConfig = new Config("APP", "2018-01-01", 0, 3);
    private final MapStateDescriptor<String, Map<String, UserEventContainer>> userMapStateDesc =
            new MapStateDescriptor<String, Map<String, UserEventContainer>>(
                    "UserEventContainerState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    new MapTypeInfo<String, UserEventContainer>(String.class, UserEventContainer.class));

    /**
     * 负责处理非广播流中的传入元素
     *
     * @param userEvent
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement(UserEvent userEvent, ReadOnlyContext context, Collector<EvaluatedResult> collector) throws Exception {
        // 获取用户id
        String userId = userEvent.getUserId();
        // 获取渠道
        String channel = userEvent.getChannel();
        // 获取事件类型
        EventType eventType = EventType.valueOf(userEvent.getEventType());

        // 根据渠道 channel 获取广播变量
        Config config = context.getBroadcastState(Launcher.configStateDescriptor).get(channel);

        // 判断广播变量是否为空
        if (Objects.isNull(config)) {
            config = defaultConfig;
        }

        // <channel,UserEventContainer>
        final MapState<String, Map<String, UserEventContainer>> state = getRuntimeContext().getMapState(userMapStateDesc);

        // <userId,UserEventContainer>
        Map<String, UserEventContainer> userEventContainerMap = state.get(channel);

        // 判断 userEventContainerMap 是否为空
        if (Objects.isNull(userEventContainerMap)) {
            userEventContainerMap = new HashMap<>();
            state.put(channel, userEventContainerMap);
        }

        // 判断 userId 是否为空
        if (!userEventContainerMap.containsKey(userId)) {
            UserEventContainer container = new UserEventContainer();
            container.setUserId(userId);
            userEventContainerMap.put(userId, container);
        }
        // map 容器添加 userEvent
        userEventContainerMap.get(userId).getUserEvents().add(userEvent);

        if (eventType == EventType.PURCHASE) {
            // 计算用户购买路径
            Optional<EvaluatedResult> result = computer(config, userEventContainerMap.get(userId));
            result.ifPresent(r -> collector.collect(result.get()));
            state.get(channel).remove(userId);
        }
    }

    /**
     * 负责处理广播流中的传入元素(例如规则)，一般把广播流中的元素添加到状态里去备用，
     * processElement 处理业务数据时就可以使用(规则)
     *
     * @param config
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(Config config, Context context, Collector<EvaluatedResult> collector) throws Exception {
        // 获取渠道
        String channel = config.getChannel();
        BroadcastState<String, Config> broadcastState = context.getBroadcastState(Launcher.configStateDescriptor);

        // 更新config value for key
        broadcastState.put(channel, config);
    }

    private Optional<EvaluatedResult> computer(Config config, UserEventContainer container) {

        // 避免出现空指针
        Optional<EvaluatedResult> result = Optional.empty();

        // 获取渠道
        String channel = config.getChannel();

        // 历史购买次数
        Integer historyPurchaseTimes = config.getHistoryPurchaseTimes();

        // 最大购买路径
        Integer maxPurchasePathLength = config.getMaxPurchasePathLength();

        // 当前购买路径
        int purchasePathLength = container.getUserEvents().size();

        // historyPurchaseTimes 历史购买次数 < 10 的属于新用户
        if (purchasePathLength > maxPurchasePathLength && historyPurchaseTimes < 10) {
            // 事件根据时间排序
            container.getUserEvents().sort(Comparator.comparingLong(UserEvent::getEventTime));

            // 定义一个 map 集合
            final Map<String, Integer> stat = Maps.newHashMap();

            container.getUserEvents()
                    .stream()
                    .collect(Collectors.groupingBy(UserEvent::getEventType))
                    .forEach((eventType, events) -> stat.put(eventType, events.size()));

            final EvaluatedResult evaluatedResult = new EvaluatedResult();
            evaluatedResult.setUserId(container.getUserId());
            evaluatedResult.setChannel(channel);
            evaluatedResult.setEventTypeCounts(stat);
            evaluatedResult.setPurchasePathLength(purchasePathLength);
            result = Optional.of(evaluatedResult);

        }
        return result;
    }
}
