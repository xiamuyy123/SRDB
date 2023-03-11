package com.yq.srdb.backend.dm.pageindex;

public class PageInfo {
    //页号
    public int pageNo;
    //空闲空间
    public int freeSpace;

    public PageInfo(int pageNo, int freeSpace) {
        this.pageNo = pageNo;
        this.freeSpace = freeSpace;
    }
}