package com.hackdiary.pig;


import org.apache.pig.data.Tuple;
import org.junit.Test;

import java.io.IOException;

public class RedisLoader_test {

    @Test
    public void redis_read_load_stuff() throws IOException {

        RedisLoader loader = new RedisLoader("bacon:stuff","localhost", 6379);

        Tuple t = loader.getNext();
        System.out.println(t.toDelimitedString(","));

    }

}
