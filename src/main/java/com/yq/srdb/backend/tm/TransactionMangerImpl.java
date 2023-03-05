package com.yq.srdb.backend.tm;

import com.yq.srdb.backend.common.Error;
import com.yq.srdb.backend.utils.Panic;
import com.yq.srdb.backend.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionMangerImpl implements TransactionManger{

    //XID文件长度
    static final int XID_HEADER_LENGTH = 8;

    //XID事务状态长度
    static final int XID_STATUS_SIZE = 1;
    //状态码
    private static final byte TX_STATUS_AVTIVE = 0;
    private static final byte TX_STATUS_COMMITTED = 1;
    private static final byte TX_STATUS_ABORTED = 2;

    //超级事务
    public static final long SUPER_XID = 0;
    //文件后缀
    public static final String XID_SUFFIX = ".xid";


    //随机访问文件(.xid文件）
    private RandomAccessFile file;
    //NIO的FileChannel，将文件直接映射至用户空间（jvm），无需经过内核空间，提高效率
    private FileChannel fc;
    //xid计数器
    private long xidCounter;
    //计数器锁
    private Lock counterLock;

    public TransactionMangerImpl(RandomAccessFile randomAccessFile, FileChannel fileChannel) {
        file = randomAccessFile;
        fc = fileChannel;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    private void checkXIDCounter(){
        //1.计算文件长度
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e) {
            Panic.panic(Error.BadXIDFileException);
        }
        if(fileLen<XID_HEADER_LENGTH){
            Panic.panic(Error.BadXIDFileException);
        }
        //2.计算当前XIDcounter所在偏移量
        //1)获取XIDCounter
        ByteBuffer byteBuffer = ByteBuffer.allocate(XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(byteBuffer);

        } catch (IOException e) {
            e.printStackTrace();
        }
        this.xidCounter = Parser.parseLong(byteBuffer.array());
        //2)计算偏移量
        long offset = getXIDPosition(this.xidCounter+1);
        //3.对比fileLen和offset
        if(fileLen!=offset){
            Panic.panic(Error.BadXIDFileException);
        }


    }

    //根据xid获取在文件中的偏移量
    private long getXIDPosition(long xid){
        return (xid-1)*XID_STATUS_SIZE + XID_HEADER_LENGTH;

    }

    @Override
    public long begin() {
        counterLock.lock();

        try{
            //1.XidCounter+1状态设置为active
            long xid = this.xidCounter+1;
            updateTXStatus(xid,TX_STATUS_AVTIVE);
            //2.更新XidCounter并写回xid文件头部
            incurXIDCounter();
            //3.返回新的xid
            return xid;
        }finally {
            counterLock.unlock();
        }

    }

    private void incurXIDCounter() {
        //1.自增1
        this.xidCounter++;
        //2.写回文件header
        ByteBuffer byteBuffer = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(byteBuffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //更新后强制同步缓存到文件中
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private void updateTXStatus(long xid, byte status) {
        //1.根据xid获取偏移量
        long offset = getXIDPosition(xid);
        //2.将status写入对应位置
        byte[] bytes = new byte[XID_STATUS_SIZE];
        bytes[0]=status;
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        try {
            fc.position(offset);
            fc.write(byteBuffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //更新后强制同步缓存到文件中
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

    }

    @Override
    public void commit(long xid) {
        updateTXStatus(xid,TX_STATUS_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateTXStatus(xid,TX_STATUS_ABORTED);
    }

    private boolean checkTXStatus(long xid,byte status){
        //1.获取xid对应状态
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[XID_STATUS_SIZE]);
        long offset = getXIDPosition(xid);
        try {
            fc.position(offset);
            fc.read(byteBuffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //2.判断是否相同
        if(byteBuffer.array()[0]==status){
            return true;
        }else{
            return false;
        }

    }
    @Override
    public boolean isActive(long xid) {
        return checkTXStatus(xid,TX_STATUS_AVTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        return checkTXStatus(xid,TX_STATUS_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        return checkTXStatus(xid,TX_STATUS_ABORTED);
    }

    @Override
    public void close() {

    }
}
