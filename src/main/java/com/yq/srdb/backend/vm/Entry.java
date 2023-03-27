package com.yq.srdb.backend.vm;

import com.google.common.primitives.Bytes;
import com.yq.srdb.backend.common.SubArray;
import com.yq.srdb.backend.dm.dataitem.DataItem;
import com.yq.srdb.backend.utils.Parser;

import java.util.Arrays;

//[XMIN] [XMAX] [DATA]
public class Entry {

    //XMIN起始位置
    private static final int OFFSET_XMIN = 0;
    //XMAX起始位置
    private static final int OFFSET_XMAX = OFFSET_XMIN+8;
    //DATA起始位置
    private static final int OFFSET_DATA = OFFSET_XMAX+8;

    //key值
    private long uid;
    //对应的dt,即该entry为所在dt的data部分
    private DataItem dataItem;
    private VersionManager vm;

    public long getUid() {
        return uid;
    }
    public void release() {
        ((VersionManagerImpl)vm).releaseEntry(this);
    }
    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }
    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    public void remove() {
        dataItem.release();
    }

    //封装data为Entry格式
    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }
    // 以拷贝的形式返回内容
    public byte[] data() {
        dataItem.rLock();
        try {
            //dataItem 的data段
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OFFSET_DATA];
            //截取16开始的数据
            System.arraycopy(sa.raw, sa.start+OFFSET_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }
    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start+OFFSET_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }

    public long getXmin() {
        dataItem.rLock();
        try{
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw,sa.start+OFFSET_XMIN,sa.start+OFFSET_XMAX));
        }finally {
            dataItem.rUnLock();
        }
    }

    public long getXmax() {
        dataItem.rLock();
        try{
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw,sa.start+OFFSET_XMAX,sa.start+OFFSET_DATA));
        }finally {
            dataItem.rUnLock();
        }
    }
}