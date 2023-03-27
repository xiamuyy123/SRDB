package com.yq.srdb.backend.im;

import com.yq.srdb.backend.common.SubArray;
import com.yq.srdb.backend.dm.dataitem.DataItem;
import com.yq.srdb.backend.tm.TransactionManagerImpl;
import com.yq.srdb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 * son指向子节点，为node id，key为索引字段值，非叶子节点为右区间的最小值
 */
public class Node {
    static final int IS_LEAF_OFFSET = 0;
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET+1;
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET+2;
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET+8;

    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2);
    //b+树的引用
    BPlusTree tree;
    //对于的dt
    DataItem dataItem;
    //该节点对应的实际字节数据
    SubArray raw;
    //node id
    long uid;

    //通过uid加载node
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    //插入结点并分裂
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();
        dataItem.before();
        try {
            success = insert(uid, key);
            //插入失败
            if(!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            //若插入后已满则需要分裂
            if(needSplit()) {
                try {
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch(Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if(err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }
    }

    //结点分裂，把右半部分分离成新结点
    private SplitRes split() throws Exception {
        //空结点
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        //是否叶子
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        //一半结点
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        //原来的兄弟
        setRawSibling(nodeRaw, getRawSibling(raw));
        //拷贝右边一半的子结点
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);

        //插入新的结点数据并生成node
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        //更新子结点数
        setRawNoKeys(raw, BALANCE_NUMBER);
        //更新兄弟
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }
    //拷贝kth及之后的子结点信息
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start+NODE_HEADER_SIZE, from.end-offset);
    }
    //判断该结点是否需要分裂
    private boolean needSplit() {
        return BALANCE_NUMBER*2 == getRawNoKeys(raw);
    }

    //在当前node插入一个子结点
    private boolean insert(long uid, long key) {
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        while(kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if(ik < key) {
                kth ++;
            } else {
                break;
            }
        }
        //到达最右侧且有兄弟,即所插入结点的key超过该结点范围
        if(kth == noKeys && getRawSibling(raw) != 0) return false;
        //key应该插入到kth之前
        //若为叶子结点，插到kth左侧
        if(getRawIfLeaf(raw)) {
            //后移kth及其后面结点
            shiftRawKth(raw, kth);
            //将所插入的结点放到合适位置
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            //更新子结点个数
            setRawNoKeys(raw, noKeys+1);
        } else { //非叶子结点，插到kth右侧
            long kk = getRawKthKey(raw, kth);
            //更新kth的key为后一个的最小值，即该区间的上限
            setRawKthKey(raw, key, kth);
            //后移kth+1及其后面结点
            shiftRawKth(raw, kth+1);
            //将所插入的结点放到kth+1处
            setRawKthKey(raw, kk, kth+1);
            setRawKthSon(raw, uid, kth+1);
            //更新子结点个数
            setRawNoKeys(raw, noKeys+1);
        }
        return true;
    }
    //将kth所在位置和其后面所有子结点信息向后移动16字节
    static void shiftRawKth(SubArray raw, int kth) {
        //第kth+1个子结点的起始位置
        int begin = raw.start+NODE_HEADER_SIZE+(kth+1)*(8*2);
        //终点
        int end = raw.start+NODE_SIZE-1;
        //从后向前拷贝
        for(int i = end; i >= begin; i --) {
            raw.raw[i] = raw.raw[i-(8*2)];
        }
    }

    //searchNext 寻找对应 key 的 UID,即确定key所在的node, 如果找不到, 则返回兄弟节点的 UID
    public SearchNextRes searchNext(long key){
        dataItem.rLock();
        try{
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);
            for (int i = 0; i < noKeys; i++) {
                //获取第i个key
                long ik = getRawKthKey(raw,i);
                //若key<ik，则key在其对应的子节点内
                if(key<ik){
                    res.uid = getRawKthSon(raw,i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;
        }finally {
            dataItem.rUnLock();
        }

    }

    /**
     * 在当前节点进行范围查找，范围是 [leftKey, rightKey]，
     * 这里约定如果 rightKey 大于等于该节点的最大的 key, 则还同时返回兄弟节点的 UID，方便继续搜索下一个节点。
     */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try{
            int noKeys = getRawNoKeys(raw);
            //找到左边界
            int kth = 0;
            while(kth < noKeys){
                long ik = getRawKthKey(raw,kth);
                if(ik>=leftKey){
                    break;
                }
                kth++;
            }
            List<Long> uids = new ArrayList<>();
            //遍历直到右边界所在key
            while(kth < noKeys){
                long ik = getRawKthKey(raw,kth);
                if(ik<=rightKey){
                    //拿到key对应的node id
                    uids.add(getRawKthSon(raw,kth));
                    kth++;
                }else {
                    break;
                }
            }
            long siblingUid = 0;
            if(kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;

        }finally {
            dataItem.rUnLock();
        }
    }

    //判断是否为页子节点
    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }


    //获取isLeaf字段
    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte)1;
    }

    //生成根节点，初始两个子节点left和right，编号为0、1，key为key和max
    static byte[] newRootRaw(long left,long right,long key){
        SubArray raw = new SubArray(new byte[NODE_SIZE],0,NODE_SIZE);
        setRawIsLeaf(raw,false);
        //设置子结点个数
        setRawNoKeys(raw, 2);
        //设置兄弟节点id
        setRawSibling(raw, 0);
        //设置left
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        //设置right
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);
        return raw.raw;
    }
    //生成空的根
    static byte[] newNilRootRaw()  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(raw, true);
        //设置子结点个数
        setRawNoKeys(raw, 0);
        //设置兄弟节点id
        setRawSibling(raw, 0);
        return raw.raw;
    }
    public void release() {
        dataItem.release();
    }
    //获取子结点个数
    static int getRawNoKeys(SubArray raw) {
        return (int)Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start+NO_KEYS_OFFSET, raw.start+NO_KEYS_OFFSET+2));
    }
    //获取兄弟节点id
    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start+SIBLING_OFFSET, raw.start+SIBLING_OFFSET+8));
    }
    //获取第kth个子节点id
    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }
    //获取第kth个子节点key
    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    //设置第kth个子节点的key
    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }
    //设置第kth个子节点
    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    //设置兄弟节点
    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start+SIBLING_OFFSET, 8);
    }
    //设置子节点个数
    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short)noKeys), 0, raw.raw, raw.start+NO_KEYS_OFFSET, 2);
    }

    //设置叶子节点标志位
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if(isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)0;
        }
    }
    class SearchNextRes {
        long uid;
        long siblingUid;
    }
    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }
    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }
    class SplitRes {
        long newSon, newKey;
    }

}
