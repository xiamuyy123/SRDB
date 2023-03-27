package com.yq.srdb.server;

import org.junit.Test;

public class LauncherTest {
    @Test
    public void test(){
        Launcher.createDB(Launcher.DB_PREFIX+"srdb");
    }
    @Test
    public void test1(){
        Launcher.openDB(Launcher.DB_PREFIX+"srdb",100*Launcher.KB);
    }
}
