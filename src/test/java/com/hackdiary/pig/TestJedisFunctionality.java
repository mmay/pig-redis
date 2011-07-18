package com.hackdiary.pig;

import org.junit.Test;
import redis.clients.jedis.Jedis;

public class TestJedisFunctionality {

    @Test
    public void testJedis() {
        Jedis jedis = new Jedis("localhost", 6379);
        jedis.set("bacon:1", "val1");
        jedis.set("bacon:2", "val2");

        String keyPattern = "bacon:*";
        System.out.println(jedis.get("bacon:*"));
        System.out.println(jedis.hkeys("bacon:"));
        System.out.println(jedis.hgetAll("bacon"));

        System.out.println(jedis.keys("bacon:*"));

        jedis.hset("bacon:stuff", "date1", "100");
        jedis.hset("bacon:stuff", "date2", "200");

        System.out.println(jedis.hgetAll("bacon:stuff"));
        System.out.println(jedis.hget("bacon:stuff", "date1"));



    }

}
