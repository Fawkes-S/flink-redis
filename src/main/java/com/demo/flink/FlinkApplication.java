package com.demo.flink;

import com.alibaba.fastjson.JSONObject;
import com.demo.flink.model.ClickEvent;
import com.demo.flink.model.Order;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class FlinkApplication {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取source stream
        DataStream<Object> source = sourceStream(env).filter(value -> !StringUtils.isEmpty(value))
                .map(new MapFunction<String, List<Object>>() {
                    @Override
                    public List<Object> map(String value) throws Exception {
                        JSONObject binlog = JSONObject.parseObject(value);
                        if (!"insert".equalsIgnoreCase(binlog.getString("type"))) {
                            return null;
                        }
                        String tableName = binlog.getString("table");
                        // 只计算订单表与点击事件表的数据
                        if (!"t_order".equalsIgnoreCase(tableName) && !"t_click_event".equalsIgnoreCase(tableName)) {
                            return null;
                        }
                        List<Object> result = new ArrayList<>();
                        if ("t_order".equalsIgnoreCase(tableName)) {
                            for (Object item : binlog.getJSONArray("data")) {
                                JSONObject jo = (JSONObject) item;
                                Order order = new Order();
                                order.setUserId(jo.getLong("user_id"));
                                order.setOrderId(jo.getLong("order_id"));
                                order.setOrderStatus(jo.getString("order_status"));
                                order.setCarType(jo.getString("car_type"));
                                order.setTimestamp(jo.getLong("timestamp"));
                                result.add(order);
                            }
                        }
                        if ("t_click_event".equalsIgnoreCase(tableName)) {
                            for (Object item : binlog.getJSONArray("data")) {
                                JSONObject jo = (JSONObject) item;
                                ClickEvent order = new ClickEvent();
                                order.setUserId(jo.getLong("user_id"));
                                order.setTimestamp(jo.getLong("timestamp"));
                                result.add(order);
                            }
                        }
                        return result;
                    }
                })
                .filter(Objects::nonNull) // 过滤掉null数据
                .flatMap(new FlatMapFunction<List<Object>, Object>() {
                    @Override
                    public void flatMap(List<Object> valueList, Collector<Object> out) throws Exception {
                        for (Object val : valueList) {
                            out.collect(val);
                        }
                    }
                });
        // 处理用户订单事件
        // 流处理都有哪些方法
        source.filter((FilterFunction<Object>) value -> value instanceof Order)
                .map((MapFunction<Object, Order>) value -> (Order) value)
                .keyBy((KeySelector<Order, Object>) Order::getCarType)
                .map((MapFunction<Order, Event>) value -> {
                    Event event = new Event();
                    event.setMetric(value.getCarType());
                    event.setTimestamp(value.getTimestamp());
                    event.setUserId(value.getUserId());
                    return event;
                })
                .addSink(new HashRedisSink());
        // 处理用户点击事件
        source.filter((FilterFunction<Object>) value -> value instanceof ClickEvent)
                .map((MapFunction<Object, ClickEvent>) value -> (ClickEvent) value)
                .map((MapFunction<ClickEvent, Event>) value -> {
                    Event event = new Event();
                    event.setMetric("click");
                    event.setTimestamp(value.getTimestamp());
                    event.setUserId(value.getUserId());
                    return event;
                })
                .addSink(new KvRedisSink());
        System.out.println(env.getExecutionPlan());
        env.execute("Kafka Consumer");
    }

    private static DataStream<String> sourceStream(StreamExecutionEnvironment env) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "my-group");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("example", new SimpleStringSchema(), props);
        return env.addSource(consumer).setParallelism(4);
    }

    public static class KvRedisSink extends RichSinkFunction<Event> {
        private JedisPool jedisPool;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 使用默认的redis配置
            jedisPool = new JedisPool();
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            String redisKey = value.getUserId() + "_" + value.getMetric();

            try (Jedis jedis = jedisPool.getResource()) {
                jedis.incr(redisKey);
            } catch (Exception ex) {
                System.out.println("写入redis失败");
            }
        }
    }
}
