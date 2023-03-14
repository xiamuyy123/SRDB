package com.yq.srdb;

import com.yq.srdb.backend.tm.TransactionManager;

import java.nio.ByteBuffer;

public class Test {
    public static void main(String[] args) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putLong(15000);
        System.out.println(ByteBuffer.wrap(byteBuffer.array(),0,8).getLong());
//        TransactionManger.create("test");
//        TransactionMangerImpl transactionManger = TransactionManger.open("test");
//        long tx = transactionManger.begin();
//        System.out.println(transactionManger.isActive(tx));
//        transactionMa
        TransactionManager.create("test1");

    }


}
