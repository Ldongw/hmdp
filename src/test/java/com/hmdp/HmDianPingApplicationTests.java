package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.*;

/**
 * @author A
 * @date 2025/8/29
 **/
@SpringBootTest
public class HmDianPingApplicationTests {

    @Autowired
    private ShopServiceImpl shopService;
    @Autowired
    private CacheClient cacheClient;
    @Autowired
    private RedisIdWorker redisIdWorker;

    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Test
    public void testSaveShop(){
        Shop shop = shopService.getById(1L);
        cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY + 1L, shop, 10L, TimeUnit.MINUTES);
    }

    @Test
    public void testIdWorker() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(300);
        long begin = System.currentTimeMillis();
        for(int i = 0; i < 300; i++)
            es.submit(()->{
                for(int j = 0; j < 100; j++){
                    System.out.println(redisIdWorker.nextId("order"));
                }
                countDownLatch.countDown();
            });
        countDownLatch.await();
        long end = System.currentTimeMillis();
        System.out.println(" time = " + (end - begin));
    }

    @Test
    void loadShopData(){
        List<Shop> list = shopService.list();
        Map<Long, List<Shop>> map = list.stream()
                .collect(Collectors.groupingBy(Shop::getTypeId));
        for(Map.Entry<Long, List<Shop>> entry : map.entrySet()){
            Long typeId = entry.getKey();
            String key = SHOP_GEO_KEY + typeId;
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = value.stream()
                    .map(o -> new RedisGeoCommands.GeoLocation<>(o.getId().toString(), new Point(o.getX(), o.getY())))
                    .collect(Collectors.toList());

            stringRedisTemplate.opsForGeo().add(key, locations);
        }
    }
    @Test
    void testHyperLogLog(){
        String[] values = new String[1000];
        for(int i = 0; i < 100000; i++){
            int j = i % 1000;
            values[j] = "user_" + j;
            if(j == 999) stringRedisTemplate.opsForHyperLogLog().add("hl1", values);
        }
        System.out.println(stringRedisTemplate.opsForHyperLogLog().size("hl1"));
    }
}
