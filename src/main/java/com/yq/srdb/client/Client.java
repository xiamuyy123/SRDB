package com.yq.srdb.client;

import com.yq.srdb.transport.Package;
import com.yq.srdb.transport.Packager;


public class Client {
    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    public byte[] execute(byte[] stat) throws Exception {
        //包装
        Package pkg = new Package(stat, null);
        //发送数据拿到执行结果
        Package resPkg = rt.roundTrip(pkg);
        if(resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }

}
