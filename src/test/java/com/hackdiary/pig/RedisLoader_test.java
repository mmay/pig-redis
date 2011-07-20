package com.hackdiary.pig;


import com.hackdiary.pig.jedis_impl.JedisHashSetRecordReader;
import com.hackdiary.pig.jedis_impl.JedisInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.pig.data.Tuple;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class RedisLoader_test {

    @Test
    public void redis_read_load_stuff() throws IOException, InterruptedException {

        Jedis jedis = new Jedis("localhost", 6379);
        jedis.hset("bacon:stuff", "date1", "1000");
        jedis.hset("bacon:stuff", "date2", "2000");
        jedis.hset("bacon:stuff", "date3", "3000");
        jedis.hset("bacon:stuff", "date4", "4000");
        jedis.hset("bacon:stuff", "date5", "5000");

        Map<String, String> hSet = jedis.hgetAll("bacon:stuff");
        Iterator<Map.Entry<String, String>> iterator = hSet.entrySet().iterator();
        jedis.disconnect();

        RedisLoader loader = new RedisLoader("bacon:stuff", "localhost", "6379");

        Configuration conf = new Configuration();
        conf.set(JedisInputFormat.jedisHost, "localhost");
        conf.set(JedisInputFormat.jedisPost, "6379");
        TaskAttemptID id = new TaskAttemptID();
        TaskAttemptContext context = new TaskAttemptContext(conf, id);
        JedisHashSetRecordReader reader = new JedisHashSetRecordReader();
        reader.initialize(null, context);
        loader.prepareToRead(reader,null);

        Tuple t = null;
        while (iterator.hasNext()) {
            t = loader.getNext();
            if (t != null) {
                System.out.println(t.toDelimitedString(","));
            }
        }
        assertNotNull(t);

    }

}
