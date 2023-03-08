package com.yq.srdb.backend.dm.page;

import com.yq.srdb.backend.dm.cache.PageCache;
import com.yq.srdb.backend.utils.Parser;

import java.util.Arrays;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class DataPage {
    private static final short OFFSET_FREE = 0;
    private static final short OFFSET_LEN = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OFFSET_LEN; //最大空闲空间

    //插入raw到page中
    public int insert(Page page,byte[] raw){
        page.setDirty(true);
        if(getFreeSpaceSize(page)<raw.length){
            return -1;
        }
        short offset = getFSOFromPage(page);
        System.arraycopy(raw,0,page.getData(),0,raw.length);
        setFSO(page.getData(), (short) (offset+raw.length));
        return offset;
    }
    //在数据库崩溃后重新打开时，恢复例程直接插入数据以及修改数据使用
    // 将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
    public static void recoverInsert(Page page,byte[] raw,short offset){
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);

        short rawFSO = getFSOFromByte(page.getData());
        if(rawFSO < offset + raw.length) {
            setFSO(page.getData(), (short)(offset+raw.length));
        }
    }
    // 将raw插入pg中的offset位置，不更新update
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
    //将偏移量写入byte数组
    public static void setFSO(byte[] raw,short offset){
        System.arraycopy(Parser.short2Byte(offset),0,raw,0,OFFSET_LEN);
    }
    //获取page的FSO（free space offset）
    public static short getFSOFromPage(Page page){
        return getFSOFromByte(page.getData());

    }
    public static short getFSOFromByte(byte[] raw){
        return Parser.byte2Short(raw);

    }
    //获取页面空闲空间大小
    public static int getFreeSpaceSize(Page page){
        return MAX_FREE_SPACE - (int)getFSOFromPage(page);

    }


}
