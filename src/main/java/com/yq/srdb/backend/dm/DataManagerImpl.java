package com.yq.srdb.backend.dm;

import com.yq.srdb.backend.common.Error;
import com.yq.srdb.backend.dm.cache.AbstractCache;
import com.yq.srdb.backend.dm.cache.PageCache;
import com.yq.srdb.backend.dm.dataitem.DataItem;
import com.yq.srdb.backend.dm.logger.Logger;
import com.yq.srdb.backend.dm.page.DataPage;
import com.yq.srdb.backend.dm.page.FirstPage;
import com.yq.srdb.backend.dm.page.Page;
import com.yq.srdb.backend.dm.pageindex.PageIndex;
import com.yq.srdb.backend.dm.pageindex.PageInfo;
import com.yq.srdb.backend.tm.TransactionManager;
import com.yq.srdb.backend.utils.Panic;
import com.yq.srdb.backend.utils.Types;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    //事务管理
    TransactionManager tm;
    //页面缓存
    PageCache pageCache;
    //日志
    Logger logger;
    //页面索引
    PageIndex pIndex;
    //第一页
    Page pageOne;

    public DataManagerImpl(PageCache pageCache, Logger logger, TransactionManager tm) {
        super(0);
        this.pageCache = pageCache;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }
    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }
    @Override
    public DataItem read(long uid) throws Exception {
        DataItem dataItem = super.get(uid);
        if(dataItem.isValid()){
            return dataItem;
        }else{
            dataItem.release();
            return null;
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        //封装为DataItem结构
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > DataPage.MAX_FREE_SPACE){
            throw Error.DataTooLargeException;
        }
        //先获取PageInfo
        PageInfo pageInfo = null;
        //尝试获取
        for (int i = 0; i < 5; i++) {
            pageInfo = pIndex.select(raw.length);
            if(pageInfo!=null){
                break;
            }else{
                int newPageNo = pageCache.newPage(DataPage.initRaw());
                pIndex.add(newPageNo,DataPage.MAX_FREE_SPACE);
            }
        }
        //无空闲页面
        if(pageInfo == null){
            throw Error.DatabaseBusyException;
        }
        Page page = null;
        int freeSize = 0;
        try{
            page = pageCache.getPage(pageInfo.pageNo);
            //做日志
            byte[] log = Recover.insertLog(xid, page, raw);
            logger.log(log);
            //执行插入操作
            int offset = DataPage.insert(page, raw);
            page.release();
            return Types.addressToUid(pageInfo.pageNo, (short) offset);
        }finally {
            // 将取出的pg重新插入pIndex
            if(page != null) {
                pIndex.add(pageInfo.pageNo, DataPage.getFreeSpaceSize(page));
            } else {
                pIndex.add(pageInfo.pageNo,freeSize);
            }

        }

    }

    @Override
    protected DataItem getForCache(long key) throws Exception {
        //根据uid解析页号和偏移量
        short offset = (short) (key & (1L<<16)-1);//取后16位
        key>>>=32;
        int pageNo = (int) (key & (1L<<32)-1);//取前16位
        Page page = pageCache.getPage(pageNo);
        return DataItem.parseDataItem(page,offset,this);
    }

    @Override
    protected void releaseForCache(DataItem dataItem) {
        dataItem.page().release();
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        FirstPage.copyVCWhenClose(pageOne);
        pageOne.release();
        pageCache.close();
    }

    @Override
    public void release(DataItem dataItem) {
        super.release(dataItem.getUid());
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno = pageCache.newPage(FirstPage.init());
        assert pgno == 1;
        try {
            pageOne = pageCache.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pageCache.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pageCache.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return FirstPage.checkVC(pageOne);
    }
    // 初始化pageIndex
    void fillPageIndex() {
        //从缓存里拿到所有页面加入页面空闲索引中
        int pageNumber = pageCache.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pageCache.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), DataPage.getFreeSpaceSize(pg));
            pg.release();
        }
    }
}
