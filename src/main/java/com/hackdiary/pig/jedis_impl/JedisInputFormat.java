package com.hackdiary.pig.jedis_impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.impl.util.UDFContext;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JedisInputFormat extends InputFormat<String, String> {
    private static final Log LOG = LogFactory.getLog(JedisInputFormat.class);
    public static final String jedisHost = "jedis.host";
    public static final String jedisPort = "jedis.port";
    public static final String redisHashSetName = "redis.hashset.name";

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        LOG.error("In GET SPLITS ===================");
        List<InputSplit> splits = new ArrayList<InputSplit>();
        UDFContext context = UDFContext.getUDFContext();
        Properties p = context.getClientSystemProps();
        LOG.error("==========" + p.getProperty(jedisHost));
        LOG.error("==========" + p.getProperty(jedisPort));
        LOG.error("==========" + p.getProperty(redisHashSetName));

        Jedis jedis = new Jedis(p.getProperty(jedisHost), Integer.parseInt(p.getProperty(jedisPort)));

        Map map = jedis.hgetAll(p.getProperty(redisHashSetName));

        splits.add(new JedisInputSplit(map));
        return splits;
    }

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        JedisHashSetRecordReader jedisRR = new JedisHashSetRecordReader();
        jedisRR.initialize(inputSplit, taskAttemptContext);
        return jedisRR;
    }
}
