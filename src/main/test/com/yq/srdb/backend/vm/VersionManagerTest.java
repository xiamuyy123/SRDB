package com.yq.srdb.backend.vm;

import com.yq.srdb.backend.dm.DataManager;
import com.yq.srdb.backend.dm.cache.PageCache;
import com.yq.srdb.backend.tm.TransactionManager;
import com.yq.srdb.backend.utils.Parser;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class VersionManagerTest {

    //删除
    @Test
    public void test4() throws Exception {
        assert new File("tmp/VersionManagerTest.db").delete();
        assert new File("tmp/VersionManagerTest.log").delete();
        assert new File("tmp/VersionManagerTest.xid").delete();

        TransactionManager tm = TransactionManager.create("tmp/VersionManagerTest");
        DataManager dm = DataManager.create("tmp/VersionManagerTest", PageCache.PAGE_SIZE*10,tm);
        VersionManager vm = VersionManager.newVersionManager(tm, dm);
        byte[] data = Parser.int2Byte(16);
        long t = vm.begin(0);
        long ins = vm.insert(t, data);
        byte[] read = vm.read(t, ins);
        System.out.println("---初始值:"+Parser.parseInt(read));
        vm.delete(t,ins);
        read = vm.read(t, ins);
        System.out.println("---删除后："+read);
        vm.commit(t);


        TimeUnit.SECONDS.sleep(10);

    }
    //隔离级别
    @Test
    public void test3() throws Exception {
        assert new File("tmp/VersionManagerTest.db").delete();
        assert new File("tmp/VersionManagerTest.log").delete();
        assert new File("tmp/VersionManagerTest.xid").delete();

        TransactionManager tm = TransactionManager.create("tmp/VersionManagerTest");
        DataManager dm = DataManager.create("tmp/VersionManagerTest", PageCache.PAGE_SIZE*10,tm);
        VersionManager vm = VersionManager.newVersionManager(tm, dm);
        byte[] data = Parser.int2Byte(16);
        long t = vm.begin(0);
        long ins = vm.insert(t, data);
        byte[] read = vm.read(t, ins);
        System.out.println("---初始值:"+Parser.parseInt(read));
        long t1 = vm.begin(0);
        byte[] read1 = vm.read(t1, ins);
        System.out.println(read1);
        vm.commit(t);
        vm.commit(t1);

        TimeUnit.SECONDS.sleep(10);

    }
    //多线程
    @Test
    public void test() throws Exception {
        assert new File("tmp/VersionManagerTest.db").delete();
        assert new File("tmp/VersionManagerTest.log").delete();
        assert new File("tmp/VersionManagerTest.xid").delete();

        TransactionManager tm = TransactionManager.create("tmp/VersionManagerTest");
        DataManager dm = DataManager.create("tmp/VersionManagerTest", PageCache.PAGE_SIZE*100,tm);
        VersionManager vm = VersionManager.newVersionManager(tm, dm);
        for (int i = 0; i < 2000; i++) {
            int finalI = i;
            new Thread(
                    ()->{
                        for (int j = 0; j < 100; j++) {
                            long tx = vm.begin(0);
                            byte[] data = Parser.int2Byte(finalI +16);
                            long insert = 0;
                            try {
                                insert = vm.insert(tx, data);
                                byte[] read = vm.read(tx, insert);
                                System.out.println(Parser.parseInt(read));
                                vm.commit(tx);
                        } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
            ).start();
        }
        TimeUnit.SECONDS.sleep(100);

    }
    //单线程
    @Test
    public void test1() throws Exception {
        assert new File("tmp/VersionManagerTest.db").delete();
        assert new File("tmp/VersionManagerTest.log").delete();
        assert new File("tmp/VersionManagerTest.xid").delete();

        TransactionManager tm = TransactionManager.create("tmp/VersionManagerTest");
        DataManager dm = DataManager.create("tmp/VersionManagerTest", PageCache.PAGE_SIZE*10,tm);
        VersionManager vm = VersionManager.newVersionManager(tm, dm);
        long tx = vm.begin(0);

        byte[] data = Parser.int2Byte( 16);
        long insert = 0;
        insert = vm.insert(tx, data);
        byte[] read = vm.read(tx, insert);
        System.out.println(Parser.parseInt(read));
        vm.commit(tx);
        TimeUnit.SECONDS.sleep(10);

    }
}
