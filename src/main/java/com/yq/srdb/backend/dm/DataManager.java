package com.yq.srdb.backend.dm;

import com.yq.srdb.backend.dm.cache.PageCache;
import com.yq.srdb.backend.dm.dataitem.DataItem;
import com.yq.srdb.backend.dm.logger.Logger;
import com.yq.srdb.backend.dm.page.FirstPage;
import com.yq.srdb.backend.tm.TransactionManager;

public interface DataManager {
    //获取DataItem
    DataItem read(long uid) throws Exception;
    //插入一条
    long insert(long xid, byte[] data) throws Exception;
    //关闭
    void close();
    //释放
    void release(DataItem dataItem);

    //从空文件创建dm
    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        FirstPage.setVCWhenOpen(dm.pageOne);
        dm.pageCache.flushPage(dm.pageOne);
        return dm;
    }
}
