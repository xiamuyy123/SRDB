package com.yq.srdb.server;

import com.yq.srdb.backend.dm.DataManager;
import com.yq.srdb.backend.dm.cache.PageCache;
import com.yq.srdb.backend.tbm.TableManager;
import com.yq.srdb.backend.tm.TransactionManager;
import com.yq.srdb.backend.vm.VersionManager;
import org.junit.Test;

import java.io.File;

public class ExecutorTest {
    @Test
    public void test() throws Exception {
        assert new File("tmp/server/ExecutorTest.db").delete();
        assert new File("tmp/server/ExecutorTest.log").delete();
        assert new File("tmp/server/ExecutorTest.xid").delete();
        assert new File("tmp/server/ExecutorTest.bt").delete();
        TransactionManager tm = TransactionManager.create("tmp/server/ExecutorTest");
        DataManager dm = DataManager.create("tmp/server/ExecutorTest", PageCache.PAGE_SIZE*100,tm);
        VersionManager vm = VersionManager.newVersionManager(tm, dm);
        TableManager tbm = TableManager.create("tmp/server/ExecutorTest", vm, dm);
        Executor executor = new Executor(tbm);
        byte[] begin = "begin".getBytes();
        byte[] commit = "commit".getBytes();
        byte[] create = "create table test id int32 ,name string , age int32 (index id)".getBytes();
        byte[] insert  = "insert into test values 1 aaa 12".getBytes();
        byte[] select = "select * from test where id > 0".getBytes();
        executor.execute(begin);
        executor.execute(create);
        executor.execute(insert);
        byte[] execute = executor.execute(select);
        System.out.println(new String(execute));
        executor.execute(commit);
    }
}
