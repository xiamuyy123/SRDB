package com.yq.srdb.backend.tbm;

import com.yq.srdb.backend.dm.DataManager;
import com.yq.srdb.backend.parser.statement.*;
import com.yq.srdb.backend.utils.Parser;
import com.yq.srdb.backend.vm.VersionManager;

public interface TableManager {
    //开启事务
    BeginRes begin(Begin begin);
    //提交事务
    byte[] commit(long xid) throws Exception;
    //撤销事务
    byte[] abort(long xid);

    //show table
    byte[] show(long xid);
    //创建表
    byte[] create(long xid, Create create) throws Exception;
    //插入
    byte[] insert(long xid, Insert insert) throws Exception;
    //读取
    byte[] read(long xid, Select select) throws Exception;
    //更新
    byte[] update(long xid, Update update) throws Exception;
    //删除
    byte[] delete(long xid, Delete delete) throws Exception;

    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
