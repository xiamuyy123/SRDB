package com.yq.srdb.backend.vm;

import com.yq.srdb.backend.common.Error;
import com.yq.srdb.backend.dm.DataManager;
import com.yq.srdb.backend.dm.cache.AbstractCache;
import com.yq.srdb.backend.tm.Transaction;
import com.yq.srdb.backend.tm.TransactionManager;
import com.yq.srdb.backend.tm.TransactionManagerImpl;
import com.yq.srdb.backend.utils.Panic;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    //事务管理器
    TransactionManager tm;
    //数据管理器
    DataManager dm;
    //活跃事务
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;
    public VersionManagerImpl(int maxResource) {
        super(maxResource);
    }
    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    @Override
    protected Entry getForCache(long key) throws Exception {
        Entry entry = Entry.loadEntry(this, key);
        if(entry == null){
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        //调用dm的release方法，释放所在dt
        entry.remove();
    }

    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();
        if(transaction.err!=null){
            throw transaction.err;
        }
        Entry e = super.get(uid);
        try{
            //可见性判断
            if(Visibility.isVisible(tm,transaction,e)){
                return e.data();
            }else {
                return null;
            }
        }finally {
            e.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        //将数据包装为Entry，交给dm插入
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();
        if(transaction.err!=null){
            throw transaction.err;
        }
        byte[] entry = Entry.wrapEntryRaw(xid,data);
        return dm.insert(xid,entry);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        //1.可见性判断
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();
        if(transaction.err!=null){
            throw transaction.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }
        try {
            if(!Visibility.isVisible(tm, transaction, entry)) {
                return false;
            }
            //2.获取资源的锁
            Lock l = null;
            try {
                //尝试获取资源
                l = lt.add(xid, uid);
                //死锁
            } catch(Exception e) {
                transaction.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                transaction.autoAborted = true;
                throw transaction.err;
            }
            //等待锁不为空，需要阻塞
            if(l != null) {
                //阻塞，等到其他事务释放资源
                l.lock();
                l.unlock();
            }
            //该记录已经被该事务删除了
            if(entry.getXmax() == xid) {
                return false;
            }
            //是否发生版本跳跃
            if(Visibility.isVersionSkip(tm, transaction, entry)) {
                transaction.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                transaction.autoAborted = true;
                throw transaction.err;
            }
            //删除该记录
            entry.setXmax(xid);
            return true;

        } finally {
            entry.release();
        }
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try{
            long xid = tm.begin();
            Transaction transaction = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid,transaction);
            return xid;
        }finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();
        try{
            if(transaction.err!=null){
                throw transaction.err;
            }
        }catch (NullPointerException e){
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(e);
        }
        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();
        //在locktable中移除该事务
        lt.remove(xid);
        tm.commit(xid);


    }
    //自动/手动撤销事务
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();
        if(t.autoAborted) return;
        //在locktable中移除该事务
        lt.remove(xid);
        tm.abort(xid);
    }
    @Override
    public void abort(long xid) {
        internAbort(xid,false);
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }
}
