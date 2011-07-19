package com.hackdiary.pig.jedis_impl;


import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;

public class JedisInputSplit extends InputSplit{
    @Override
    public long getLength() throws IOException, InterruptedException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
    }
}
