package com.yq.srdb.backend.tm;

import java.util.HashMap;
import java.util.Map;

public class Transaction {
    //事务id
    public long xid;

    public int level;
    //当前事务开启时的活跃事务快照
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    //新建事务
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if(level != 0) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    //是否处于快照中
    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}