package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.core.KeyHelper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.time.ZonedDateTime;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;
    private final String name;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;

        this.name = String.valueOf(ZonedDateTime.now().toInstant().toEpochMilli());
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        // START CHALLENGE #7
        try (Jedis jedis = jedisPool.getResource()) {
            String key = getRateSlidingLimiterKey(this.name, this.windowSizeMS, this.maxHits);

            Pipeline p = jedis.pipelined();

            long currTimestampInMs = ZonedDateTime.now().toInstant().toEpochMilli();
            p.zadd(key, currTimestampInMs,
                    KeyHelper.getKey(String.valueOf(currTimestampInMs) + ":" + String.valueOf(Math.random())));
            p.zremrangeByScore(key, 0, currTimestampInMs - this.windowSizeMS);
            Response<Long> count = p.zcard(key);
            p.sync();

            if (count.get() > maxHits) {
                throw new RateLimitExceededException();
            }
        }
        // END CHALLENGE #7
    }

    private String getRateSlidingLimiterKey(String name, long windowSize, long maxHits) {
        return KeyHelper.getKey("limiter:" +
                String.valueOf(windowSize) + ":" +
                name + ":" +
                String.valueOf(maxHits));
    }
}
