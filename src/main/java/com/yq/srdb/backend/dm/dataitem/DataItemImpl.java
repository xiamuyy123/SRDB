package com.yq.srdb.backend.dm.dataitem;

import com.yq.srdb.backend.common.SubArray;
import com.yq.srdb.backend.dm.DataManager;
import com.yq.srdb.backend.dm.DataManagerImpl;
import com.yq.srdb.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem{

    //ValidFlag位偏移量
    static final int OFFSET_VALID = 0;
    //DATASIZE位偏移量
    static final int OFFSET_SIZE = 1;
    //Data偏移量
    static final int OFFSET_DATA = 3;
    //共享数组,raw.raw是所在页的所有数据
    private SubArray raw;
    //存放旧的dataitem数据
    private byte[] oldRaw;
    //读锁
    private Lock rLock;
    //写锁
    private Lock wLock;
//    private DataManagerImpl dm;
    private long uid;
    private Page page;
    private DataManager dm;

    public DataItemImpl(SubArray subArray, byte[] bytes, Page page, long uid, DataManagerImpl dm) {
        this.raw = subArray;
        this.oldRaw = bytes;
        this.page = page;
        this.uid = uid;
        this.dm = dm;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();

    }

    @Override
    //是否有效
    public boolean isValid(){
        return  raw.raw[raw.start+OFFSET_VALID] == (byte) 0;
    }

    @Override
    public SubArray data() {
        return new SubArray(raw.raw,raw.start+OFFSET_DATA, raw.end);
    }

    //更新数据时的前向操作，保存前相数据
    @Override
    public void before() {
        wLock.lock();
        //设所在页为脏页
        page.setDirty(true);
        //保存前相数据
        System.arraycopy(raw.raw,0,this.oldRaw,0,oldRaw.length);
    }
    //撤销更新时的回滚操作，恢复前相数据
    @Override
    public void unBefore() {
        System.arraycopy(oldRaw,0,raw.raw,raw.start,oldRaw.length);
        wLock.unlock();
    }

    //更新后的写日志操作
    @Override
    public void after(long xid) {
        wLock.unlock();
    }

    @Override
    public void release() {
        dm.release(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return page;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
