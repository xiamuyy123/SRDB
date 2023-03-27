package com.yq.srdb.backend.utils;

public class ParseStringRes {
    public String str;
    //下一个String起始位置
    public int next;

    public ParseStringRes(String str, int next) {
        this.str = str;
        this.next = next;
    }
}
