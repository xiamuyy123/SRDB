package com.yq.srdb.backend.dm.cache;

import com.yq.srdb.backend.common.Error;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

//引用计数法
public abstract class AbstractCache<T> {

    //资源缓存
    private Map<Long,T> cache;

    //资源引用计数
    private Map<Long,Integer> references;

    //资源是否正在从数据源获取（避免多线程下重复获取）
    private Set<Long> getting;

    //缓存资源最大数
    private int maxResource;
    //当前缓存资源数
    private int count;
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashSet<>();
        lock = new ReentrantLock();
    }
    //资源不存在时获取
    protected abstract T getForCache(long key) throws Exception;

    //资源驱逐时的写回
    protected abstract void releaseForCache(T t);

    protected T get(long key) throws Exception {
        while (true){
            lock.lock();
            //缓存有，直接返回
            if(cache.containsKey(key)){
                lock.unlock();
                T t = cache.get(key);
                references.put(key,references.get(key)+1);
                return t;
            }else{
                //缓存无
                //先上锁
                //有其他线程正在获取资源

                if(getting.contains(key)){

                    lock.unlock();
                    continue;
                    //没有其他线程正在获取资源
                }else{
                    //尝试获取该资源
                    if(maxResource>0&&count==maxResource){
                        throw Error.CacheFullException;
                    }
                    count++;
                    //设置为true
                    getting.add(key);
                    //获取资源
                    T t = null;
                    try{
                        t = getForCache(key);
                    }catch (Exception e){
                        //回滚
                        count--;
                        getting.remove(key);
                        lock.unlock();
                        throw e;
                    }
                    //放入缓存
                    cache.put(key,t);
                    //getting设为false
                    getting.remove(key);
                    references.put(key,1);
                    lock.unlock();
                    return t;

                }
            }

        }

    }
    //强行释放缓存，先对引用-1，若为0则释放
    protected void release(long key){
        lock.lock();
        try{
            int ref = references.get(key)-1;

            //引用数为0（无人使用）
            if(ref==0){
                releaseForCache(cache.get(key));
                references.remove(key);
                cache.remove(key);
                count--;
            }else{
                references.put(key,ref);
            }
        }finally {
            lock.unlock();
        }

    }
    //安全关闭,释放资源
    protected void close(){
        lock.lock();
        try{
            Set<Long> keySet = cache.keySet();
            for (Long key : keySet) {
                release(key);
                cache.remove(key);
                references.remove(key);
            }
        }finally {
            lock.unlock();
        }

    }

}
