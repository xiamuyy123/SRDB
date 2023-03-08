package com.yq.srdb.backend.dm.page;

import com.yq.srdb.backend.dm.cache.PageCache;
import com.yq.srdb.backend.utils.RandomUtil;

import java.util.Arrays;
/**
 * 特殊管理第一页
 * ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 */
public class FirstPage {
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    //初始化数据
    public static byte[] init(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        System.arraycopy(RandomUtil.randomBytes(LEN_VC),0,raw,OF_VC,LEN_VC);
        return raw;
    }

    //启动时设置初始随机版本序列
    public static void setVCWhenOpen(Page page){
        page.setDirty(true);
        System.arraycopy(RandomUtil.randomBytes(LEN_VC),0,page.getData(),OF_VC,LEN_VC);
    }
    //关闭时拷贝版本序列
    public static void copyVCWhenClose(Page page){
        page.setDirty(true);
        System.arraycopy(page.getData(),OF_VC,page.getData(),OF_VC+LEN_VC,LEN_VC);
    }
    //校验版本序列
    public static boolean checkVC(Page page){
        return Arrays.equals(Arrays.copyOfRange(page.getData(),OF_VC,OF_VC+LEN_VC),Arrays.copyOfRange(page.getData(),OF_VC+LEN_VC,OF_VC+LEN_VC*2));

    }
}
