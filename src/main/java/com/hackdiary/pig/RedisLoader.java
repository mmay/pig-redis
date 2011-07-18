package com.hackdiary.pig;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class RedisLoader extends LoadFunc {
    protected Jedis jedis;
    protected RecordReader in = null;
    private String loadLocation;
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();
    private String key;
    protected String host;
    protected int port;
    private Map<String, String> hSet;
    protected Iterator<Map.Entry<String,String>> hSetIterator;

    public RedisLoader(String key, String host) {
        this(key,host,6379);
    }

    public RedisLoader(String key, String host, int port) {
        this.key = key;
        this.host = host;
        this.port = port;

    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        loadLocation = location;
        FileInputFormat.setInputPaths(job, location);
    }

    @SuppressWarnings("unchecked")
    @Override
    public InputFormat getInputFormat() throws IOException {
        return new PigTextInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
        in = recordReader;
        jedis = new Jedis(host, port);
        hSet = jedis.hgetAll(key);
        hSetIterator = hSet.entrySet().iterator();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Tuple getNext() throws IOException {
        if (!hSetIterator.hasNext()) {
            return null;
        }
        Map.Entry<String, String> entry = hSetIterator.next();

        return tupleFactory.newTuple(entry);
    }
}
