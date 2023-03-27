package com.yq.srdb.backend.tbm;

import com.yq.srdb.backend.dm.DataManager;
import com.yq.srdb.backend.dm.cache.PageCache;
import com.yq.srdb.backend.parser.statement.*;
import com.yq.srdb.backend.tm.TransactionManager;
import com.yq.srdb.backend.vm.VersionManager;
import org.junit.Test;

import java.io.File;

public class TableManagerTest {
    @Test
    public void test1() throws Exception {
        assert new File("tmp/TableManagerTest.db").delete();
        assert new File("tmp/TableManagerTest.log").delete();
        assert new File("tmp/TableManagerTest.xid").delete();
        assert new File("tmp/TableManagerTest.bt").delete();
        TransactionManager tm = TransactionManager.create("tmp/TableManagerTest");
        DataManager dm = DataManager.create("tmp/TableManagerTest", PageCache.PAGE_SIZE*100,tm);
        VersionManager vm = VersionManager.newVersionManager(tm, dm);
        TableManager tbm = TableManager.create("tmp/TableManagerTest", vm, dm);
        Begin begin = new Begin();
        begin.isRepeatableRead=false;
        Create create = new Create();
        //create table test id(int32),name(string),age(int 32);
        create.tableName = "test";
        create.fieldName = new String[]{"id","name","age"};
        create.fieldType = new String[]{"int32","string","int32"};
        create.index = new String[]{"id"};
        //insert into table values(12,hhh,7);
        Insert insert = new Insert();
        insert.tableName = "test";
        insert.values = new String[]{"12","hhh","8"};
        Insert insert1 = new Insert();
        insert1.tableName = "test";
        insert1.values = new String[]{"11","aaa","9"};
        //select id,name,age from test where id = 12;
        Select select = new Select();
        select.tableName = "test";
        select.fields = new String[]{"id","name","age"};
        Where where = new Where();
        SingleExpression s1 = new SingleExpression();
        s1.field = "id";
        s1.compareOp = ">";
        s1.value = "10";
        where.singleExp1 = s1;
        where.logicOp =  "";
        select.where = where;
        //delete from test where id = 11
        Delete delete = new Delete();
        delete.tableName = "test";
        where = new Where();
        s1 = new SingleExpression();
        s1.field = "id";
        s1.compareOp = "=";
        s1.value = "11";
        where.singleExp1 = s1;
        where.logicOp =  "";
        delete.where = where;


        BeginRes beginRes = tbm.begin(begin);
        long xid = beginRes.xid;
        tbm.create(xid,create );
        tbm.insert(xid,insert);
        tbm.insert(xid,insert1);
        byte[] read = tbm.read(xid, select);



        String res = new String(read);
        System.out.println(res);
        tbm.delete(xid,delete);
        System.out.println("----删除后----");
        read = tbm.read(xid, select);
        res = new String(read);
        System.out.println(res);



    }
}
