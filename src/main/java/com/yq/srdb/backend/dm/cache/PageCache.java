package com.yq.srdb.backend.dm.cache;

import com.yq.srdb.backend.common.Error;
import com.yq.srdb.backend.dm.page.Page;
import com.yq.srdb.backend.utils.Panic;
import org.omg.CORBA.PUBLIC_MEMBER;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface PageCache {
    public static final int PAGE_SIZE = 1 << 13;
    //创建新页面
    int newPage(byte[] data);
    //获取页面
    Page getPage(int pageNo) throws Exception;
    //获取页面数量
    int getPageNumber();
    //释放页面
    void release(Page page);
    //关闭缓存
    void close();
    //截断
    void truncateByPageNo(int maxPageNo);
    //刷盘
    void flushPage(Page page);

    //可以考虑单例
    public static PageCache create(String path,long memory){
        File file = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            //文件是否已存在
            if(!file.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return getPageCacheFromFile(file,memory);
    }
    public static PageCache open(String path,long memory){
        File file = new File(path + PageCacheImpl.DB_SUFFIX);
        //文件是否已存在
        if(!file.exists()){
            Panic.panic(Error.FileExistsException);
        }

        return getPageCacheFromFile(file,memory);

    }
    public static PageCache getPageCacheFromFile(File file,long memory){
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl((int)memory/PageCache.PAGE_SIZE,raf,fc);
    }
}
