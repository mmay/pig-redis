package com.hackdiary.pig;

import com.hackdiary.pig.jedis_impl.JedisHashSetRecordReader;
import com.hackdiary.pig.jedis_impl.JedisInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;

import java.io.IOException;
import java.util.Properties;

/**
 * Load Func for Redis HashSets
 */
public class RedisLoader extends LoadFunc {
    private static final Log LOG = LogFactory.getLog(RedisLoader.class);
    protected JedisHashSetRecordReader recordReader;
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();
    private String key;
    protected String host;
    protected String port;

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
        LOG.error("In set location ===================");
        UDFContext context = UDFContext.getUDFContext();
        JobConf conf = (JobConf) job.getConfiguration();
        context.addJobConf(conf);
        Properties p = context.getClientSystemProps();
        p.setProperty(JedisInputFormat.jedisHost, host);
        p.setProperty(JedisInputFormat.jedisPort, port);
        p.setProperty(JedisInputFormat.redisHashSetName, key);

        context.setClientSystemProps();
        LOG.error("HOST:" + p.getProperty(JedisInputFormat.jedisHost));
        LOG.error("PORT:" + p.getProperty(JedisInputFormat.jedisPort));
        LOG.error("hset:" + p.getProperty(JedisInputFormat.redisHashSetName));
    }

    @SuppressWarnings("unchecked")
    @Override
    public InputFormat getInputFormat() throws IOException {
        LOG.error("IN getINputFOrmat ===============");
        return new JedisInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
        LOG.error("In prep to read ===================");
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
