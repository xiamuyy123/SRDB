package com.yq.srdb.backend.im;

import java.io.File;
import java.util.List;

import com.yq.srdb.backend.dm.DataManager;
import com.yq.srdb.backend.dm.cache.PageCache;
import com.yq.srdb.backend.tm.MockTransactionManager;
import com.yq.srdb.backend.tm.TransactionManager;
import org.junit.Test;



public class BPlusTreeTest {
    @Test
    public void testTreeSingle() throws Exception {
        assert new File("tmp/TestTreeSingle.db").delete();
        assert new File("tmp/TestTreeSingle.log").delete();
        TransactionManager tm = new MockTransactionManager();
        DataManager dm = DataManager.create("tmp/TestTreeSingle", PageCache.PAGE_SIZE*10, tm);

        long root = BPlusTree.create(dm);
        BPlusTree tree = BPlusTree.load(root, dm);

        int lim = 100;
        for(int i = lim-1; i >= 0; i --) {
            if(i==4){
                System.out.println(1);
            }
            tree.insert(i, i);
        }
//        tree.insert(16, 16);
//        List<Long> uids = tree.search(36);
//        System.out.println(uids.get(0));

        for(int i = 0; i < lim; i ++) {
            List<Long> uids = tree.search(i);
            System.out.println(uids.get(0));
            assert uids.size() == 1;
            assert uids.get(0) == i;
        }


    }
}
