package com.yq.srdb.backend.im;

import com.yq.srdb.backend.common.SubArray;
import com.yq.srdb.backend.dm.DataManager;
import com.yq.srdb.backend.dm.dataitem.DataItem;
import com.yq.srdb.backend.tm.TransactionManagerImpl;
import com.yq.srdb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BPlusTree {

    DataManager dm;
    //根节点id
    long bootUid;
    //维护root结点的uid
    DataItem bootDataItem;
    Lock bootLock;

    //创建索引树,返回bootdt的id
    public static long create(DataManager dm) throws Exception {
        //空根（是叶子）
        byte[] rawRoot = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }
    //根据rootId加载索引树
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }
    //获取根节点id
    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }
    //更新root,rootNode满了分裂生成left和right，需要生成新的root
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            //初始新root数据
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            //插入该root数据，生成node结点
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            //因为是新结点，所以执行before拷贝一份数据
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            //写入新的rootId
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            //落日志
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }
    //找key对应的叶子
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();
        //是叶子
        if(isLeaf) {
            return nodeUid;
        } else {//不是叶子
            //找到key对应node
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }
    private long searchNext(long nodeUid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.SearchNextRes res = node.searchNext(key);
            node.release();
            if(res.uid != 0) return res.uid;
            if(res.siblingUid==0){
                return 0;
            }
            nodeUid = res.siblingUid;
        }
    }
    //根据key查找
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }
    //范围查询
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        //左边界对应叶子节点
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while(true) {
            Node leaf = Node.loadNode(this, leafUid);
            //在叶子中查找符合条件的uid
            Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }
    //插入数据
    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        //若根结点分裂
        if(res.newNode != 0) {
            //更新root
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }
    //插入数据到叶子
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        //是叶子直接插入
        if(isLeaf) {
            res = insertAndSplit(nodeUid, uid, key);
        } else {//不是叶子
            //找到下一层node位置
            long next = searchNext(nodeUid, key);
            //递归直到叶子结点
            InsertRes ir = insert(next, uid, key);
            //如果子结点分裂
            if(ir.newNode != 0) {
                //把新结点插入当前结点
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }
    //在节点插入key
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if(iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }
    public void close() {
        bootDataItem.release();
    }
    class InsertRes {
        long newNode, newKey;
    }

}
