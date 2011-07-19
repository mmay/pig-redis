package com.hackdiary.pig.jedis_impl;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class JedisHashSetRecordReader extends RecordReader<String, String> {
    private Jedis jedis;
    private Map<String, String> HSET;
    private Iterator<Map.Entry<String,String>> HSET_Iterator;


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration conf = taskAttemptContext.getConfiguration();
        jedis = new Jedis(conf.get(JedisInputFormat.jedisHost), Integer.parseInt(conf.get(JedisInputFormat.jedisPost)));
        HSET = jedis.hgetAll(JedisInputFormat.redisHashSetName);
        HSET_Iterator = HSET.entrySet().iterator();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return HSET_Iterator.hasNext();
    }

    @Override
    public String getCurrentKey() throws IOException, InterruptedException {
        return HSET_Iterator.next().getKey();
    }

    @Override
    public String getCurrentValue() throws IOException, InterruptedException {
        return HSET_Iterator.next().getValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        jedis.disconnect();
    }
}
