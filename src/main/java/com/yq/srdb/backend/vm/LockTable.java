package com.yq.srdb.backend.vm;


import com.yq.srdb.backend.common.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {

    //资源和事务的持有关系
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有
    //资源和事务的等待关系
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;      // XID正在等待的UID
    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }
    //移除事务
    public void remove(long xid) {
        lock.lock();
        try {
            //获取xid所持有资源
            List<Long> l = x2u.get(xid);
            if(l != null) {
                while(l.size() > 0) {
                    //释放其占有的资源
                    Long uid = l.remove(0);
                    //其他等待的事务可以持有该资源
                    selectNewXID(uid);
                }
            }
            //移除队列
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);

        } finally {
            lock.unlock();
        }
    }
    // 从等待队列中选择一个xid来占用uid
    private void selectNewXID(long uid) {
        //移除占有该资源的事务
        u2x.remove(uid);
        //获取等待该资源的事务
        List<Long> l = wait.get(uid);
        if(l == null) return;
        assert l.size() > 0;

        while(l.size() > 0) {
            //拿到第一个
            long xid = l.remove(0);
            //xid没有等待锁-》即xid不需要该资源了
            if(!waitLock.containsKey(xid)) {
                //跳过
                continue;
            } else {
                //该资源被xid占有
                u2x.put(uid, xid);
                //移除并获取等待锁
                Lock lo = waitLock.remove(xid);
                //移除等待队列
                waitU.remove(xid);
                //释放等待锁
                lo.unlock();
                break;
            }
        }

        if(l.size() == 0) wait.remove(uid);
    }
    // 不需要等待则返回null，否则返回锁对象
    // 会造成死锁则抛出异常
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            //uid已经被xid获取，不需要等待
            if(isInList(x2u, xid, uid)) {
                return null;
            }
            //资源uid未被占有
            if(!u2x.containsKey(uid)) {
                //xid持有uid
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                //不需要等待
                return null;
            }
            //uid未被xid占有且uid被其他x占有-》等待
            //xid加入等待队列
            waitU.put(xid, uid);
            putIntoList(wait, uid, xid);
            //发生死锁
            if(hasDeadLock()) {
                //将该资源移除xid对应等待队列
                waitU.remove(xid);
                //将该事务移除uid对应等待队列
                removeFromList(wait, uid, xid);
                //抛出死锁异常
                throw Error.DeadlockException;
            }
            //未死锁
            Lock l = new ReentrantLock();
            l.lock();
            //加入等待锁队列
            waitLock.put(xid, l);
            return l;

        } finally {
            lock.unlock();
        }
    }
    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }
    //遍历图时存储中间结点
    private Map<Long, Integer> xidStamp;
    //邮戳号
    private int stamp;
    //死锁检测
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        //遍历x2u
        for(long xid : x2u.keySet()) {
            //对于xid
            Integer s = xidStamp.get(xid);
            //已经遍历过
            if(s != null && s > 0) {
                continue;
            }
            //联通子图数，该节点未遍历过，stamp为该节点起的资源等待连通子图邮戳号
            stamp ++;
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }
    //类似链表找环
    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        //该节点已经遍历过且在同一个资源等待子图中
        if(stp != null && stp == stamp) {
            return true;
        }
        //该节点已经遍历过但不在同一个资源等待子图中
        if(stp != null && stp < stamp) {
            return false;
        }
        //放入map
        xidStamp.put(xid, stamp);
        //获取xid所等待资源
        Long uid = waitU.get(xid);
        //没有等待的资源
        if(uid == null) return false;
        //获取持有该资源的事务
        Long x = u2x.get(uid);
        assert x != null;
        //递归遍历
        return dfs(x);
    }
    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if(!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }
    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return false;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                return true;
            }
        }
        return false;
    }
}
