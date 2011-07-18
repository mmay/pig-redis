package com.hackdiary.pig;


import org.apache.pig.data.Tuple;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class RedisLoader_test {

    @Test
    public void redis_read_load_stuff() throws IOException {

        Jedis jedis = new Jedis("localhost", 6379);
        Map<String,String> hSet = jedis.hgetAll("bacon:stuff");
        Iterator<Map.Entry<String,String>> iterator = hSet.entrySet().iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next().toString());
        }

        RedisLoader loader = new RedisLoader("bacon:stuff","localhost", 6379);
        Tuple t = loader.getNext();
        assertNotNull(t);

    }

}
