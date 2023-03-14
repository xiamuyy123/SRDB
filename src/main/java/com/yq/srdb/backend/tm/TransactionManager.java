package com.yq.srdb.backend.tm;

import com.yq.srdb.backend.common.Error;
import com.yq.srdb.backend.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface TransactionManager {
    //开启事务
    long begin();

    //提交事务
    void commit(long xid);

    //取消事务
    void abort(long xid);

    //事务是否活跃
    boolean isActive(long xid);

    //事务是否提交
    boolean isCommitted(long xid);

    //事务是否取消
    boolean isAborted(long xid);

    //关闭事务
    void close();

    //创建新的xid文件并生产txm
    static TransactionManager create(String path){
        File file = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if(!file.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(!file.canRead()||!file.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }
        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;

        try {
            randomAccessFile = new RandomAccessFile(file,"rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        //新建空header
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[TransactionManagerImpl.XID_HEADER_LENGTH]);
        try {
            fileChannel.position(0);
            fileChannel.write(byteBuffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
//        System.out.println(file.length());
        return new TransactionManagerImpl(randomAccessFile,fileChannel);


    }
    //根据已有xid文件创建txm
    static TransactionManager open(String path){
        File file = new File(path + TransactionManagerImpl.XID_SUFFIX);

        if(!file.exists()){
            Panic.panic(Error.FileNotExistsException);
        }

        if(!file.canRead()||!file.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }
        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;

        try {
            randomAccessFile = new RandomAccessFile(file,"rw");
            fileChannel = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        try {
            fileChannel.position(0);
            ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[TransactionManagerImpl.XID_HEADER_LENGTH]);
            fileChannel.read(byteBuffer);

        } catch (IOException e) {
            e.printStackTrace();
        }


        return new TransactionManagerImpl(randomAccessFile,fileChannel);


    }
}
