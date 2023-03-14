package com.yq.srdb.backend.dm.pageindex;

import com.yq.srdb.backend.dm.cache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class PageIndex {

    //区间数
    private static final int INTERVAL_NUMS = 40;
    //区间大小
    private static final int INTERVAL_SIZE = PageCache.PAGE_SIZE/INTERVAL_NUMS;

    //存放各个区间空闲空间的页面信息，例如lists[1]就是freeSpace<=INTERVAL_SIZE 的所有页面信息；
    private List<PageInfo>[] lists ;

    private Lock lock;

    public PageIndex(){
        lock =  new ReentrantLock();
        lists = new List[INTERVAL_NUMS+1];
        for(int i =0;i<INTERVAL_NUMS+1;i++){
            lists[i] = new ArrayList<>();
        }
    }

    //从空闲页列表中找出大于等于spaceSize的页
    public PageInfo select(int spaceSize){
        int number = spaceSize/INTERVAL_SIZE;
        //向上取整
        if(number<INTERVAL_NUMS){
            number++;
        }
        while (number<=INTERVAL_NUMS){
            //此区间没有空闲空间
            if(lists[number].size()==0){
                number++;
                continue;
            }
            //会直接移除该页面-》不允许并发写页面，用完再归还
            return lists[number].remove(0);
        }
        return null;
    }

    public void add(int pageNo,int freeSpace){
        lock.lock();
        try{
            int number = freeSpace/INTERVAL_SIZE;        //向上取整
            if(number<INTERVAL_NUMS){
                number++;
            }

            lists[number].add(new PageInfo(pageNo,freeSpace));
        }finally {
            lock.unlock();
        }


    }


}
