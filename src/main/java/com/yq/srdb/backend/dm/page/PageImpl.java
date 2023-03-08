package com.yq.srdb.backend.dm.page;


import com.yq.srdb.backend.dm.cache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageImpl implements Page{

    private Lock lock;
    private int pageNumber;
    private byte[] data;
    private boolean dirty;

    private PageCache pageCache;

    public PageImpl(int pageNumber, byte[] data, PageCache pageCache) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pageCache = pageCache;
        lock = new ReentrantLock();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        lock.lock();
        try{
            pageCache.release(this);
        }finally {
            lock.unlock();
        }

    }

    @Override
    public void setDirty(boolean dirty) {
        lock.lock();
        try{
            this.dirty=true;
        }finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isDirty() {
        return this.dirty;
    }

    @Override
    public int getPageNumber() {
        return this.pageNumber;
    }

    @Override
    public byte[] getData() {
        return this.data;
    }
}
