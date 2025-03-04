package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 锁管理器，用于管理每个页面的锁。
 */
public class LockManager {

    // 将每个 PageId 映射到锁列表，一个页面可能有多个锁。
    private Map<PageId, List<Lock>> lockMap;

    public LockManager() {
        this.lockMap = new ConcurrentHashMap<>(); // 支持多线程操作
    }

    /**
     * 当前事务尝试为页面设置锁。
     *
     * @param pageId   表 ID
     * @param tid      事务 ID
     * @param lockType 锁类型
     * @return 返回锁是否设置成功
     */
    public synchronized boolean setLock(PageId pageId, TransactionId tid, int lockType) {
        // 1. 如果页没有锁，则根据参数类型为页面创建锁
        if (!lockMap.containsKey(pageId)) {
            Lock lock = new Lock(tid, lockType);
            List<Lock> locks = new ArrayList<>();
            locks.add(lock);
            lockMap.put(pageId, locks);
            return true;
        }

        // 2. 如果页面有锁
        List<Lock> locks = lockMap.get(pageId);
        for (Lock lock : locks) {
            // 2.1 如果当前事务已经持有该页面的锁
            if (lock.getTid().equals(tid)) {
                // 如果该事务在该页面上已经有相同或者更高级别的锁，则无需做任何操作，返回 true
                if (lock.getLockType() == lockType || lock.getLockType() == 1) {
                    return true;
                }
                // 如果传入锁类型为写锁，已有锁为读锁且只有一个锁，则将读锁升级为写锁。
                if (lock.getLockType() == 0 && locks.size() == 1) {
                    lock.setLockType(1);
                    return true;
                }
                // 否则返回 false，表示无法添加锁。
                return false;
            }
        }

        // 2.2 如果当前事务还未持有该页面的锁

        // 如果页面已经有写锁，则加锁失败
        if (!locks.isEmpty() && locks.get(0).getLockType() == 1) {
            return false;
        }

        // 如果页面没有读锁，且当前事务要加的锁为读锁，则加锁成功。
        if (lockType == 0) {
            Lock lock = new Lock(tid, lockType);
            locks.add(lock);
            return true;
        }
        return false;
    }

    /**
     * 释放当前事务在指定页面上的锁。
     *
     * @param pageId
     * @param tid
     */
    public synchronized void releaseLock(PageId pageId, TransactionId tid) {
        // 如果页面没有锁，直接返回
        if (!lockMap.containsKey(pageId)) {
            return;
        }

        // 如果页面有锁
        List<Lock> locks = lockMap.get(pageId);
        for (Lock lock : locks) {
            // 如果页面的锁属于该事务，则移除锁
            if (lock.getTid().equals(tid)) {
                locks.remove(lock);
                lockMap.put(pageId, locks);
                if (locks.isEmpty()) {
                    lockMap.remove(pageId);
                }
                return;
            }
        }
    }

    /**
     * 判断给定事务是否持有指定页面的锁。
     *
     * @param pageId
     * @param tid
     * @return
     */
    public synchronized boolean holdsLock(PageId pageId, TransactionId tid) {
        // 如果锁映射中不包含该页面，则当前事务必定没有锁
        if (!lockMap.containsKey(pageId)) {
            return false;
        }

        // 获取页面的锁
        List<Lock> locks = lockMap.get(pageId);
        for (Lock lock : locks) {
            // 检查当前事务是否包含锁
            if (lock.getTid().equals(tid)) {
                return true;
            }
        }
        return false;
    }
}
