package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/**
 * @author A
 * @date 2025/8/29
 **/
@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        this.stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Long time, TimeUnit unit, Function<ID, R> dbFallback){
        String key = keyPrefix + id;

        String json = stringRedisTemplate.opsForValue().get(key);

        if(StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json, type);
        }

        if(json != null)
            return null;

        R r = dbFallback.apply(id);

        if(r == null){
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }

        this.set(key, r, time, unit);
        return r;
    }


    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type,  Long time, TimeUnit unit, Function<ID, R> dbFallBack){
        String key = keyPrefix + id;

        String json = stringRedisTemplate.opsForValue().get(key);

        if(StrUtil.isBlank(json)){
            return null;
        }

        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();

        if(expireTime.isAfter(LocalDateTime.now()))
            return r;
        //互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);

        if(isLock){
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    // Double Check: 再次检查缓存是否仍然过期
                    String checkJson = stringRedisTemplate.opsForValue().get(key);
                    if(StrUtil.isNotBlank(checkJson)) {
                        RedisData checkData = BeanUtil.toBean(checkJson, RedisData.class);
                        LocalDateTime checkExpireTime = checkData.getExpireTime();
                        // 如果缓存已经更新（未过期），则不需要重建
                        if(checkExpireTime.isAfter(LocalDateTime.now()))
                            return;
                    }
                    R r1 = dbFallBack.apply(id);
                    this.setWithLogicalExpire(key, r1, time, unit);
                }catch (Exception e){
                    throw new RuntimeException();
                }finally {
                    unlock(lockKey);
                }
            });
        }

        return r;
    }


    public <R, ID> R queryWithMutex(String keyPrefix, ID id, Class<R> type, Long time, TimeUnit unit, Function<ID, R> dbFallBack){
        String key = keyPrefix + id;

        String json = stringRedisTemplate.opsForValue().get(key);

        if(StrUtil.isNotBlank(json))
            return JSONUtil.toBean(json, type);

        if(json != null)
            return null;

        //互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        R r = null;
        try {
            boolean isLock = tryLock(lockKey);

            if(!isLock){
                Thread.sleep(50L);
                return queryWithMutex(keyPrefix, id, type, time, unit, dbFallBack);
            }

            json = stringRedisTemplate.opsForValue().get(key);

            if(StrUtil.isNotBlank(json)){
                r = JSONUtil.toBean(json, type);
                return r;
            }

            if(json != null)
                return null;

            r = dbFallBack.apply(id);

            if(r == null){
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }

            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(r), time, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unlock(lockKey);
        }
        return r;
    }

}
