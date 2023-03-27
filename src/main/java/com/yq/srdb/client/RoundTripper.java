package com.yq.srdb.client;


import com.yq.srdb.transport.Package;
import com.yq.srdb.transport.Packager;

//发送数据，接受结果
public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
