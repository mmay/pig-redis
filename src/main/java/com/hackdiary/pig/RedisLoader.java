package com.hackdiary.pig;

import com.hackdiary.pig.jedis_impl.JedisHashSetRecordReader;
import com.hackdiary.pig.jedis_impl.JedisInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Load Func for Redis HashSets
 */
public class RedisLoader extends LoadFunc {
    protected JedisHashSetRecordReader recordReader;
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();
    private String key;
    protected String host;
    protected String port;
    private static final Logger LOG = LoggerFactory.getLogger(RedisLoader.class);

    public RedisLoader(String key, String host) {
        this(key,host,"6379");
    }

    public RedisLoader(String key, String host, String port) {
        this.key = key;
        this.host = host;
        this.port = port;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        conf.set(JedisInputFormat.jedisHost, host);
        conf.set(JedisInputFormat.jedisPost, port);
        conf.set(JedisInputFormat.redisHashSetName, key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public InputFormat getInputFormat() throws IOException {
        return new JedisInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
        this.recordReader = (JedisHashSetRecordReader) recordReader;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Tuple getNext() throws IOException {
        try {
            if (recordReader.nextKeyValue()) {
                String val = recordReader.getCurrentValue();
                return tupleFactory.newTuple(val);
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        return null;
    }
}
