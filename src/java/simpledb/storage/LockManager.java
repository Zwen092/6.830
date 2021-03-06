package simpledb.storage;

import simpledb.common.Debug;
import simpledb.transaction.TransactionId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

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

    private final Map<TransactionId, PageId> transactionIdPageIdMap;

    public LockManager() {
        lockMap = new ConcurrentHashMap<>();
        transactionIdPageIdMap = new ConcurrentHashMap<>();
    }


    //todo::this method may be synchronized
    public synchronized boolean acquireLock(TransactionId tid, PageId pageId, LockType lockType) {

        if (lockMap.get(pageId) == null) {
            //no lock in this page, grant it
            Lock lock = new Lock(tid, lockType);
            List<Lock> locks = new ArrayList<>();
            locks.add(lock);
            lockMap.put(pageId, locks);
            transactionIdPageIdMap.put(tid, pageId);
            return true;
        }
        List<Lock> locks = lockMap.get(pageId);
        /*
        one transaction repeatedly request the lock on the same page, may lead to lock upgrade or fail
         */
        for (Lock lock : locks) {
            if (lock.transactionId == tid) {
                //same lock, grant
                if (lock.lockType == lockType) {
                    return true;
                }
                //exclusive lock, grant
                if (lock.lockType == LockType.EXCLUSIVE_LOCK) {
                    return true;
                }
                //shared lock with size = 1, upgrade
                if (locks.size() == 1) {
                    lock.lockType = LockType.EXCLUSIVE_LOCK;
                    return true;
                } else {
                    //shared lock, size != 1, requesting xLock, reject
                    return false;
                }



            }
        }
        /*
        another transaction coming to request a lock on the same page
         */

        if (locks.size() == 1 && locks.get(0).lockType == LockType.EXCLUSIVE_LOCK)
            return false;
        else {
            if (lockType == LockType.SHARED_LOCK) {
                Lock newLock = new Lock(tid, lockType);
                locks.add(newLock);
                transactionIdPageIdMap.put(tid, pageId);
                return true;
            } else {
                return false;
            }
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        List<Lock> l = lockMap.get(pid);
        Iterator<Lock> iterator = l.iterator();

        while (iterator.hasNext()) {
            Lock lock = iterator.next();
            if (lock.transactionId == tid) {
                iterator.remove();
                if (l.size() == 0) {
                    releaseLocksOnaPage(pid);
                }
                return;
            }
        }
    }

    public synchronized void releaseAllLocks(TransactionId tid) {
        for (Map.Entry<PageId, List<Lock>> entry : lockMap.entrySet()) {
            PageId pageId = entry.getKey();
            if (holdsLock(tid, pageId))
                releaseLock(tid, pageId);
        }
    }

    public void releaseLocksOnaPage(PageId pageId) {

        lockMap.remove(pageId);
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId p) {
        if (lockMap.get(p) == null)
            return false;
        for (Lock lock : lockMap.get(p)) {
            if (tid == lock.transactionId)
                return true;
        }
        return false;
    }

}
