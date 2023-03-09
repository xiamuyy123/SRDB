package com.yq.srdb.backend.dm.logger;

import com.google.common.primitives.Bytes;
import com.yq.srdb.backend.common.Error;
import com.yq.srdb.backend.utils.Panic;
import com.yq.srdb.backend.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LoggerImpl implements Logger {

    //种子（计算校验和）
    private static final int SEED = 13331;

    //SIZE起始位置
    private static final int OFFSET_SIZE = 0;
    //CHECKSUM起始位置
    private static final int OFFSET_CHECKSUM = OFFSET_SIZE + 4;
    //DATA起始位置
    private static final int OFFSET_DATA = OFFSET_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    //指针位置
    private long position;
    //
    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;
    private int XCheckSum;
    private long fileSize;  // 初始化时记录，log操作不更新
    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int XChecksum) {
        this.file = raf;
        this.fc = fc;
        this.XCheckSum = XChecksum;
        lock = new ReentrantLock();
    }
    //初始化
    void init(){
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(size<4){
            Panic.panic(Error.BadLogFileException);
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(byteBuffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int XCheckSum = Parser.parseInt(byteBuffer.array());
        this.XCheckSum = XCheckSum;
        this.fileSize = size;
        checkAndRemove();
    }
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(ByteBuffer.wrap(log));
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }finally {
            lock.unlock();
        }
        updateXCheckSum(log);
    }

    //更新总校验和
    public void updateXCheckSum(byte[] log){
        this.XCheckSum = calCheckSum(this.XCheckSum,log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(this.XCheckSum)));
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
    //包装成log格式
    private byte[] wrapLog(byte[] data){
        byte[] size = Parser.int2Byte(data.length);
        byte[] checkSum = Parser.int2Byte(calCheckSum(0,data));
        return Bytes.concat(size,checkSum,data);
    }
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log, OFFSET_DATA, log.length);
        } finally {
            lock.unlock();
        }

    }

    private byte[] internNext(){
        //没有有效数据
        if((position+OFFSET_DATA)>=fileSize){
            return null;
        }
        //读取Size
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        try {
            fc.position(position+OFFSET_SIZE);
            fc.read(byteBuffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(byteBuffer.array());
        //没有有效数据
        if((position+size+OFFSET_DATA)>fileSize){
            return null;
        }

        //读取所有data
        ByteBuffer tem = ByteBuffer.allocate(OFFSET_DATA + size);
        try {
            fc.position(position);
            fc.read(tem);
        } catch (IOException e) {
            Panic.panic(e);
        }
        byte[] log = tem.array();
        //计算真实CheckSum
        int checkSum1 = calCheckSum(0, Arrays.copyOfRange(log,OFFSET_DATA,log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log,OFFSET_CHECKSUM,OFFSET_DATA));
        if(checkSum1!=checkSum1){
            return null;
        }
        position += log.length;
        return log;
    }

    private int calCheckSum(int xCheck,byte[] data) {

        for(byte b : data){
            xCheck = xCheck*SEED+b;
        }
        return xCheck;
    }

    //打开文件时校验XCheckSum并移除badtail
    private void checkAndRemove(){
        rewind();
        int xCheck = 0;
        while (true){
            byte[] log = internNext();
            if(log==null){
                break;
            }
            xCheck=calCheckSum(xCheck,log);
        }
        if(xCheck!=XCheckSum){
            Panic.panic(Error.BadLogFileException);
        }
        truncate(position);
        rewind();
    }
    @Override
    public void truncate(long offset) {
        lock.lock();
        try {
            fc.truncate(offset);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        this.position=4;
    }

    @Override
    public void close() {
        try {
            file.close();
            fc.close();
        } catch (IOException e) {
            Panic.panic(e);
        }

    }
}
