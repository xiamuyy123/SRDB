package com.yq.srdb.backend.dm.cache;

import com.yq.srdb.backend.common.Error;
import com.yq.srdb.backend.dm.page.Page;
import com.yq.srdb.backend.dm.page.PageImpl;
import com.yq.srdb.backend.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    //缓存最少页面数
    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private Lock fileLock;
    private RandomAccessFile file;
    private FileChannel fc;

    //缓存中页面数
    private AtomicInteger pageNumbers;


    public PageCacheImpl(int maxResource, RandomAccessFile file, FileChannel fc) {
        super(maxResource);
        if(maxResource<MEM_MIN_LIM){
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fc;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int)length/PAGE_SIZE);
    }

    //从文件中获取数据
    @Override
    protected Page getForCache(long key) {
        int pageNo = (int)key;
        ByteBuffer byteBuffer = ByteBuffer.allocate(PAGE_SIZE);
        long offset = pageOffset(pageNo);

        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(byteBuffer);
        } catch (IOException e) {
            Panic.panic(e);
        }finally {
            fileLock.unlock();
        }

        return new PageImpl(pageNo,byteBuffer.array(),this);
    }
    private long pageOffset(int pageNo){
        return (pageNo-1)*PAGE_SIZE;

    }
    //脏页刷盘
    @Override
    protected void releaseForCache(Page page) {
        if(page.isDirty()){
            this.flushPage(page);
            page.setDirty(false);
        }
    }

    @Override
    public int newPage(byte[] data) {
        int pageNo = pageNumbers.getAndIncrement();
        PageImpl page = new PageImpl(pageNo, data, null);
        this.flushPage(page);  // 新建的页面需要立刻写回
        return pageNo;
    }

    @Override
    public Page getPage(int pageNo) throws Exception {
        return super.get(pageNo);
    }

    @Override
    public void release(Page page) {
        super.release(page.getPageNumber());
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void truncateByPageNo(int maxPageNo) {
        long maxLength = pageOffset(maxPageNo+1);
        try {
            file.setLength(maxLength);
        } catch (IOException e) {
            e.printStackTrace();
        }
        pageNumbers.set(maxPageNo);

    }
    private int getPageNumber(){
        return this.pageNumbers.get();
    }

    @Override
    public void flushPage(Page page) {
        int pageNo = page.getPageNumber();
        long offset = pageOffset(pageNo);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.write(ByteBuffer.wrap(page.getData()));
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }finally {
            fileLock.unlock();
        }
    }
}
