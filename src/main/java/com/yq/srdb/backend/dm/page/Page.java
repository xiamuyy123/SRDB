package com.yq.srdb.backend.dm.page;

public interface Page{
    void lock();
    void unlock();
    //释放页面
    void release();
    //设置脏页面
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    //获取页面数据
    byte[] getData();


}
