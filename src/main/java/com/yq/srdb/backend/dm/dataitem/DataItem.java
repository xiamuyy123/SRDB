package com.yq.srdb.backend.dm.dataitem;

import com.google.common.primitives.Bytes;
import com.yq.srdb.backend.common.SubArray;
import com.yq.srdb.backend.dm.DataManagerImpl;
import com.yq.srdb.backend.dm.page.Page;
import com.yq.srdb.backend.utils.Parser;
import com.yq.srdb.backend.utils.Types;

import java.util.Arrays;

public interface DataItem {
    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();
    boolean isValid();
    //包装raw为DataItem结构
    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short)raw.length);
        return Bytes.concat(valid, size, raw);
    }
    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OFFSET_VALID] = (byte)1;
    }
    // 从页面的offset处解析处dataitem
    public static DataItem parseDataItem(Page page, short offset, DataManagerImpl dm) {
        byte[] raw = page.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw,offset+DataItemImpl.OFFSET_SIZE,offset+DataItemImpl.OFFSET_DATA));
        short length = (short) (size + DataItemImpl.OFFSET_DATA);
        long uid = Types.addressToUid(page.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset+length), new byte[length], page, uid, dm);
    }
}
