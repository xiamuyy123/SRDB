package com.yq.srdb.backend.vm;

import com.yq.srdb.backend.tm.Transaction;
import com.yq.srdb.backend.tm.TransactionManager;

//隔离级别可见性
public class Visibility {

    //读已提交

    /**
     * * (XMIN == Ti and                             // 由Ti创建且
     *     XMAX == NULL                            // 还未被删除
     * )
     * or                                          // 或
     * (XMIN is commited and                       // 由一个已提交的事务创建且
     *     (XMAX == NULL or                        // 尚未删除或
     *     (XMAX != Ti and XMAX is not commited)   // 由一个未提交的事务删除
     * ))
     */
    public static boolean readCommitted(TransactionManager tm, Transaction t, Entry e){
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();

        if(xmin == xid&&xmax==0){
            return true;
        }
        if(tm.isCommitted(xmin)){
            if(xmax==0){
                return true;
            }
            if(xmax!=xid&&!tm.isCommitted(xmax)){
                return true;
            }
        }
        return false;

    }
    /**
     * (XMIN == Ti and                 // 由Ti创建且
     *  (XMAX == NULL or               // 尚未被删除
     * ))
     * or                              // 或
     * (XMIN is commited and           // 由一个已提交的事务创建且
     *  XMIN < Ti and                 // 这个事务小于Ti且
     *  XMIN is not in SP(Ti) and      // 这个事务在Ti开始前提交且
     *  (XMAX == NULL or               // 尚未被删除或
     *   (XMAX != Ti and               // 由其他事务删除但是
     *    (XMAX is not commited or     // 这个事务尚未提交或
     * XMAX > Ti or                    // 这个事务在Ti开始之后才开始或
     * XMAX is in SP(Ti)               // 这个事务在Ti开始前还未提交
     * ))))
     */
    //可重复读
    public static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e){
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid&&xmax == 0){
            return true;
        }
        if(tm.isCommitted(xmin) && xmin<xid && !t.isInSnapshot(xmin)){
            if(xmax == 0){
                return true;
            }
            if(xmax!=xid){
                //xmax未提交或在当前事务之后或在当前事务开启时未提交
                if(!tm.isCommitted(xmax) || xmax>xid || t.isInSnapshot(xmax)){
                    return true;
                }
            }
        }
        return false;

    }
}
