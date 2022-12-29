package org.example.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

public class JedisPoolUtil {

    private static final JedisPool jedisPool;

    static {
        Properties props = new Properties();
        try {
            props.load(JedisPoolUtil.class.getResourceAsStream("/redis.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        String host = props.getProperty("redis.host");
        int port = Integer.parseInt(props.getProperty("redis.port"));
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(Integer.parseInt(props.getProperty("redis.pool.maxTotal")));
        config.setMaxIdle(Integer.parseInt(props.getProperty("redis.pool.maxIdle")));
        config.setMaxWait(Duration.ofMillis(Integer.parseInt(props.getProperty("redis.pool.maxWaitMillis"))));
        jedisPool = new JedisPool(config, host, port);
    }

    public static Jedis getJedis() {
        return jedisPool.getResource();
    }

    public static void close(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }
}
