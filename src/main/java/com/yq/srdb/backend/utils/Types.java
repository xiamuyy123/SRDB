package com.yq.srdb.backend.utils;

public class Types {
    //根据偏移量页号和页内偏移获取uid 4位页号+4位偏移
    public static long addressToUid(int pgno, short offset) {
        long u0 = (long)pgno;
        long u1 = (long)offset;
        return u0 << 32 | u1;
    }
}
