package com.demo.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Collections;

public class HashRedisSink extends RichSinkFunction<Event> {
    private static final Logger log = LoggerFactory.getLogger(HashRedisSink.class);

    private JedisPool jedisPool;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 使用默认的redis配置
        jedisPool = new JedisPool();
    }

    @Override
    public void invoke(Event value, Context context) throws Exception {
        String redisKey = Long.toString(value.getUserId()) + "_order";
        String hashKey = value.getMetric();
        String timeHashKey = "t";

        try (Jedis jedis = jedisPool.getResource()) {
            String checkScript = "if redis.call('exists',KEYS[1]) == 0 " +
                    "then redis.call('hset', KEYS[1], ARGV[1], 0) end";
            jedis.eval(checkScript, Collections.singletonList(redisKey), Collections.singletonList(timeHashKey));

            String script = String.format("if tonumber(redis.call('%s', '%s', '%s')) < %d then return " +
                            "redis.call('%s', '%s', '%s', 1) else return redis.error_reply('expired!') end",
                    "hget", redisKey, timeHashKey, value.getTimestamp(), "HINCRBY", redisKey, hashKey
            );
            Object result = jedis.eval(script);
            jedis.hset(redisKey, timeHashKey, value.getTimestamp().toString());
        } catch (Exception ex) {
            log.error("写入redis失败", ex);
        }
    }
}
