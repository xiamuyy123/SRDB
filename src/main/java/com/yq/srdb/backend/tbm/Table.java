package com.yq.srdb.backend.tbm;

import com.google.common.primitives.Bytes;
import com.yq.srdb.backend.common.Error;
import com.yq.srdb.backend.parser.statement.*;
import com.yq.srdb.backend.tm.TransactionManagerImpl;
import com.yq.srdb.backend.utils.Panic;
import com.yq.srdb.backend.utils.ParseStringRes;
import com.yq.srdb.backend.utils.Parser;

import java.util.*;

/**
 * Table 维护了表结构
 * 二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 */
public class Table {
    TableManager tbm;
    //所在Entry ID
    long uid;
    //表名
    String name;
    //状态
    byte status;
    //下一个表的id
    long nextUid;
    //字段集合
    List<Field> fields = new ArrayList<>();

    //头插法创建表
    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        Table tb = new Table(tbm, create.tableName, nextUid);
        for(int i = 0; i < create.fieldName.length; i ++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;
            //该字段是否要创建索引
            for(int j = 0; j < create.index.length; j ++) {
                if(fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }
            //创建字段并添加到表中
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }

        return tb.persistSelf(xid);
    }
    public static Table loadTable(TableManager tbm, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw);
    }
    //加载表时所用
    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }
    //创建表时所用
    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }
    //解析自身数据
    private Table parseSelf(byte[] raw) {
        //解析name
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        name = res.str;
        //解析下一张表id
        position += res.next;
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        position += 8;
        //解析field
        while(position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
            position += 8;
            fields.add(Field.loadField(this, uid));
        }
        return this;
    }
    //持久化
    private Table persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for(Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }
        uid = ((TableManagerImpl)tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }
    //更新 update student set name = 'aaa' where age = 10
    public int update(long xid, Update update) throws Exception {
        //确定更新的记录
        List<Long> uids = parseWhere(update.where);
        Field fd = null;
        for (Field f : fields) {
            //确定更新字段
            if(f.fieldName.equals(update.fieldName)) {
                fd = f;
                break;
            }
        }
        //不存在该字段
        if(fd == null) {
            throw Error.FieldNotFoundException;
        }
        //解析value
        Object value = fd.string2Value(update.value);
        int count = 0;
        for (Long uid : uids) {
            //读取记录
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;
            //删除前一个版本
            ((TableManagerImpl)tbm).vm.delete(xid, uid);
            //更新字段值
            Map<String, Object> entry = parseEntry(raw);
            entry.put(fd.fieldName, value);
            raw = entry2Raw(entry);
            //插入新版本数据
            long uuid = ((TableManagerImpl)tbm).vm.insert(xid, raw);

            count ++;
            //更新索引树
            for (Field field : fields) {
                if(field.isIndexed()) {
                    field.insert(entry.get(field.fieldName), uuid);
                }
            }
        }
        return count;
    }
    //插入
    public void insert(long xid, Insert insert) throws Exception {
        //将value解析为Entry格式
        Map<String, Object> entry = string2Entry(insert.values);
        //entry map->byte
        byte[] raw = entry2Raw(entry);
        //插入entry
        long uid = ((TableManagerImpl)tbm).vm.insert(xid, raw);
        for (Field field : fields) {
            //如果有索引
            if(field.isIndexed()) {
                //插入对应索引树
                field.insert(entry.get(field.fieldName), uid);
            }
        }
    }
    //查询
    public String read(long xid, Select read) throws Exception {
        List<Long> uids = parseWhere(read.where);
        StringBuilder sb = new StringBuilder();
        for (Long uid : uids) {
            //获取对应的Entry数据
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;
            //根据表结构解析entry
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }
    //删除
    public int delete(long xid, Delete delete) throws Exception {
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        for (Long uid : uids) {
            if(((TableManagerImpl)tbm).vm.delete(xid, uid)) {
                count ++;
            }
        }
        return count;
    }
    //Enrty Map -> byte[]
    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }
    //解析插入数据为Entry
    private Map<String, Object> string2Entry(String[] values) throws Exception {
        //value数必须符合字段数
        if(values.length != fields.size()) {
            throw Error.InvalidValuesException;
        }
        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            entry.put(f.fieldName, v);
        }
        return entry;
    }
    //输出一条记录
    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            //最后一个
            if(i == fields.size()-1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    //解析该表的记录数据
    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>();
        for (Field field : fields) {
            //解析字段
            Field.ParseValueRes r = field.parserValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName, r.v);
            //更新偏移量
            pos += r.shift;
        }
        return entry;
    }

    //解析where表达式 where age < 20 and age > 10
    private List<Long> parseWhere(Where where) throws Exception {
        long l0=0, r0=0, l1=0, r1=0;
        //是否单一条件 and和“” 为true，因为可以合并为一个左右边界 ，or为false
        boolean single = false;
        //有索引的字段
        Field fd = null;
        //无where条件
        if(where == null) {
            for (Field field : fields) {
                //该字段有索引
                if(field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
            //无where，范围为所有
            l0 = 0;
            r0 = Long.MAX_VALUE;
            //单表查询
            single = true;
        } else {//有where条件
            for (Field field : fields) {
                //找到左表达式字段
                if(field.fieldName.equals(where.singleExp1.field)) {
                    //无索引报错
                    if(!field.isIndexed()) {
                        throw Error.FieldNotIndexedException;
                    }
                    fd = field;
                    break;
                }
            }
            //找不到有索引的字段
            if(fd == null) {
                throw Error.FieldNotFoundException;
            }
            //确定两个条件左右边界
            CalWhereRes res = calWhere(fd, where);
            l0 = res.l0; r0 = res.r0;
            l1 = res.l1; r1 = res.r1;
            single = res.single;
        }
        List<Long> uids = fd.search(l0, r0);
        if(!single) {
            List<Long> tmp = fd.search(l1, r1);
            uids.addAll(tmp);
        }
        return uids;
    }
    //计算where表达式
    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        switch(where.logicOp) {
            //单条件
            case "":
                res.single = true;
                //确定左右边界
                FieldCalRes r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                break;
            case "or":
                res.single = false;
                //左条件左右边界
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                //右条件左右边界
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                break;
            case "and":
                res.single = true;
                //左条件左右边界
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                //左条件左右边界
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                //求交集
                if(res.l1 > res.l0) res.l0 = res.l1;
                if(res.r1 < res.r0) res.r0 = res.r1;
                break;
            default:
                throw Error.InvalidLogOpException;
        }
        return res;
    }
    class CalWhereRes {
        long l0, r0, l1, r1;
        boolean single;
    }
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for(Field field : fields) {
            sb.append(field.toString());
            if(field == fields.get(fields.size()-1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

}
