package com.yq.srdb.client;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class LauncherTest {
    @Test
    public void test(){
        Map<Integer,Integer> map = new HashMap<>();
        map.put(1,1);
        Integer i = map.get(2)-1;
    }
}
