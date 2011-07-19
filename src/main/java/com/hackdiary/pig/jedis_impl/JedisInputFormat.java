package com.hackdiary.pig.jedis_impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;

public class JedisInputFormat extends InputFormat<String, String> {
    private static final Log LOG = LogFactory.getLog(JedisInputFormat.class);
    public static String jedisHost;
    public static String jedisPost;
    public static String redisHashSetName;

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        JedisHashSetRecordReader jedisRR = new JedisHashSetRecordReader();
        jedisRR.initialize(inputSplit, taskAttemptContext);
        return jedisRR;
    }
}
