package com.yq.srdb.backend.utils;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Parser {
    //checkSum方法 校验和
    public static long str2Uid(String key) {
        long seed = 13331;
        long res = 0;
        for(byte b : key.getBytes()) {
            res = res * seed + (long)b;
        }
        return res;
    }
    //封装为FieldString格式
    public static byte[] string2Byte(String str) {
        byte[] l = int2Byte(str.length());
        return Bytes.concat(l, str.getBytes());
    }
    //解析String
    public static ParseStringRes parseString(byte[] raw) {
        int length = parseInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4+length));
        return new ParseStringRes(str, length+4);
    }
    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE/Byte.SIZE).putLong(value).array();
    }
    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(Integer.SIZE/Byte.SIZE).putInt(value).array();
    }
    public static byte[] short2Byte(short value) {
        return ByteBuffer.allocate(Short.SIZE/Byte.SIZE).putShort(value).array();
    }

    public static short byte2Short(byte[] buf) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, 2);
        return byteBuffer.getShort();
    }

    public static int parseInt(byte[] buf) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, 4);
        return byteBuffer.getInt();
    }

    public static short parseShort(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }
}
