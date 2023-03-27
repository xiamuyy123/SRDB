package com.yq.srdb.backend.tbm;

import com.google.common.primitives.Bytes;
import com.yq.srdb.backend.common.Error;
import com.yq.srdb.backend.im.BPlusTree;
import com.yq.srdb.backend.parser.statement.SingleExpression;
import com.yq.srdb.backend.tm.TransactionManagerImpl;
import com.yq.srdb.backend.utils.Panic;
import com.yq.srdb.backend.utils.ParseStringRes;
import com.yq.srdb.backend.utils.Parser;

import java.util.Arrays;
import java.util.List;

/**
 * field 表示字段信息
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid]
 * 如果field无索引，IndexUid为0
 */
public class Field {

    long uid; //所在entry id
    private Table tb; //所属table
    String fieldName; //字段名
    String fieldType; //字段类型
    private long index; //对应的索引树根结点id
    private BPlusTree bt; //对应的索引树

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }
    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }
    //创建Field并持久化
    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        //检查字段是否合法
        typeCheck(fieldType);
        Field f = new Field(tb, fieldName, fieldType, 0);
        //该字段有索引
        if(indexed) {
            long index = BPlusTree.create(((TableManagerImpl)tb.tbm).dm);
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            f.index = index;
            f.bt = bt;
        }
        //持久化
        f.persistSelf(xid);
        return f;
    }
    //类型检查，类型必须为int32或int64或string
    private static void typeCheck(String fieldType) throws Exception {
        if(!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException;
        }
    }
    //加载table的Field
    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            //获取entry数据
            raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        return new Field(uid, tb).parseSelf(raw);
    }
    //解析该Field信息
    private Field parseSelf(byte[] raw) {
        int position = 0;
        //解析FiledName
        ParseStringRes res = Parser.parseString(raw);
        fieldName = res.str;
        position += res.next;
        //解析TypeName
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = res.str;
        position += res.next;
        //解析IndexUid
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        if(index != 0) {
            try {
                bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            } catch(Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }
    public boolean isIndexed() {
        return index != 0;
    }
    //持久化该Field
    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(fieldName);
        byte[] typeRaw = Parser.string2Byte(fieldType);
        byte[] indexRaw = Parser.long2Byte(index);
        this.uid = ((TableManagerImpl)tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    //插入一条记录后更新索引树，即插入新的key和其对应uid
    public void insert(Object key, long uid) throws Exception {
        long uKey = value2Uid(key);
        bt.insert(uKey, uid);
    }
    //解析key为long
    public long value2Uid(Object key) {
        long uid = 0;
        switch(fieldType) {
            case "string":
                uid = Parser.str2Uid((String)key);
                break;
            case "int32":
                int uint = (int)key;
                return (long)uint;
            case "int64":
                uid = (long)key;
                break;
        }
        return uid;
    }
    //解析输入的条件值 输入为String解析为对应的fieldType
    public Object string2Value(String str) {
        switch(fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }
    //计算对于该字段的表达式左右边界
    public FieldCalRes calExp(SingleExpression exp) throws Exception {
        Object v = null;
        FieldCalRes res = new FieldCalRes();
        switch(exp.compareOp) {
            case "<":
                //左边界
                res.left = 0;
                v = string2Value(exp.value);
                //右边界
                res.right = value2Uid(v);
                if(res.right > 0) {
                    res.right --;
                }
                break;
            case "=":
                v = string2Value(exp.value);
                res.left = value2Uid(v);
                res.right = res.left;
                break;
            case ">":
                res.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                res.left = value2Uid(v) + 1;
                break;
        }
        return res;
    }
    //从索引树搜索
    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);
    }
    class ParseValueRes {
        Object v;
        int shift;
    }
    //解析该字段的数据
    public ParseValueRes parserValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch(fieldType) {
            case "int32":
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }
    //将该字段值转为String返回
    public String printValue(Object v) {
        String str = null;
        switch(fieldType) {
            case "int32":
                str = String.valueOf((int)v);
                break;
            case "int64":
                str = String.valueOf((long)v);
                break;
            case "string":
                str = (String)v;
                break;
        }
        return str;
    }
    //value -> byte[]
    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch(fieldType) {
            case "int32":
                raw = Parser.int2Byte((int)v);
                break;
            case "int64":
                raw = Parser.long2Byte((long)v);
                break;
            case "string":
                raw = Parser.string2Byte((String)v);
                break;
        }
        return raw;
    }
    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(", ")
                .append(fieldType)
                .append(index!=0?", Index":", NoIndex")
                .append(")")
                .toString();
    }
}
