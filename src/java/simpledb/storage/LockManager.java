package simpledb.storage;

import com.sun.javaws.ui.ApplicationIconGenerator;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;
import sun.misc.Lock;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author zwen
 * @Description
 * @create 2021-12-29 2:20 下午
 */
public class LockManager {


    public static class Lock {
        private TransactionId transactionId;
        private LockType lockType;

        public Lock(TransactionId transactionId, LockType lockType) {
            this.transactionId = transactionId;
            this.lockType = lockType;
        }
    }

    private final Map<PageId, List<Lock>> lockMap;
    private final Map<TransactionId, List<PageId>> txnMap;
    private static final LockManager lockManager = new LockManager();

    private LockManager() {
        lockMap = new ConcurrentHashMap<>();
        txnMap = new ConcurrentHashMap<>();
    }

    public static LockManager getInstance() {
        return lockManager;
    }

    public Map<TransactionId, List<PageId>> getTxnMap() {
        return txnMap;
    }

    //todo::this method may be synchronized
    public boolean acquireLock(TransactionId tid, PageId pageId, LockType lockType) {
        if (lockMap.get(pageId) == null) {
            //no lock in this page, grant it
            Lock lock = new Lock(tid, lockType);
            List<Lock> locks = new ArrayList<>();
            locks.add(lock);
            lockMap.put(pageId, locks);
            //trackTransactionIdAndPageId(tid, pageId);
            return true;
        }
        /*
        one transaction repeatedly request the lock on the same page, may lead to lock upgrade or fail
         */
        for (Lock lock : lockMap.get(pageId)) {
            if (lock.transactionId == tid) {
                //if there's only one transaction holding a shared lock, upgrade it
                if (lock.lockType == LockType.SHARED_LOCK && lockMap.get(pageId).size() == 1) {
                    lock.lockType = LockType.EXCLUSIVE_LOCK;
                    //return true;
                }
                return true;
            }
        }
        /*
        another transaction coming to request a lock on the same page
         */
        List<Lock> locks = lockMap.get(pageId);
        if (locks.size() == 1 && locks.get(0).lockType == LockType.EXCLUSIVE_LOCK)
            return false;
        else {
            if (lockType == LockType.SHARED_LOCK) {
                Lock newLock = new Lock(tid, lockType);
                locks.add(newLock);
                //trackTransactionIdAndPageId(tid, pageId);
                return true;
            } else {
                return false;
            }
        }
    }

    private void trackTransactionIdAndPageId(TransactionId tid, PageId pid) {
        if (txnMap.get(tid) == null) {
            List<PageId> pageIdList = new ArrayList<>();
            pageIdList.add(pid);
            txnMap.put(tid, pageIdList);
        } else {
            txnMap.get(tid).add(pid);
        }
    }

    public void releaseLock(TransactionId tid, PageId pid) {
        Iterator<Lock> iterator = lockMap.get(pid).iterator();
        while (iterator.hasNext()) {
            Lock l = iterator.next();
            if (l.transactionId == tid) {
                iterator.remove();
                return;
            }
        }
    }

    public void releaseLocksOnaPage(PageId pageId) {
        lockMap.remove(pageId);
    }

    public boolean holdsLock(TransactionId tid, PageId p) {
        if (lockMap.get(p) == null)
            return false;
        for (Lock lock : lockMap.get(p)) {
            if (tid == lock.transactionId)
                return true;
        }
        return false;
    }



    private boolean canGetLock() {
        return false;
    }
}
