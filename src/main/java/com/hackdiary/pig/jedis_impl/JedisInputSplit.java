package com.hackdiary.pig.jedis_impl;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.util.Map;

public class JedisInputSplit extends InputSplit{
    private static final Log LOG = LogFactory.getLog(JedisInputSplit.class);
    Map<String, String> hset;

    public JedisInputSplit(Map map) {
        this.hset = map;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        long length = 0;
        for ( Map.Entry e : hset.entrySet()) {
            long key = Byte.decode(e.getKey().toString()).longValue();
            long val = Byte.decode(e.getValue().toString()).longValue();
            length += (key + val);
        }
        LOG.error("BYTE LENGTH = " + length);
        return length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
    }
}
