package com.yq.srdb.backend.vm;

import com.yq.srdb.backend.dm.DataManager;
import com.yq.srdb.backend.tm.TransactionManager;

public interface VersionManager {
    //读取数据
    byte[] read(long xid, long uid) throws Exception;
    //插入数据
    long insert(long xid, byte[] data) throws Exception;
    //删除数据
    boolean delete(long xid, long uid) throws Exception;

    //开启事务
    long begin(int level);
    //提交事务
    void commit(long xid) throws Exception;
    //撤销事务
    void abort(long xid);
    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }
}