package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 锁管理器，用于管理每个页面的锁。
 */
public class LockManager {

    // 将每个 PageId 映射到锁列表，一个页面可能有多个锁。
    private Map<PageId, List<Lock>> lockMap;

    // 记录每个页面上等待锁的事务
    private Map<PageId, List<TransactionId>> waitingMap;

    public LockManager() {
        this.lockMap = new ConcurrentHashMap<>(); // 支持多线程操作
        this.waitingMap = new ConcurrentHashMap<>();
    }

    public synchronized boolean tryAcquireLock(PageId pageId, TransactionId tid, int lockType) {
        if (requestLock(pageId, tid, lockType)) {
            removeWaitingTransaction(pageId, tid);
            return true;
        } else {
            addWaitingTransaction(pageId, tid);
            return false;
        }
    }

    /**
     * 当前事务尝试为页面设置锁。
     *
     * @param pageId   表 ID
     * @param tid      事务 ID
     * @param lockType 锁类型
     * @return 返回锁是否设置成功
     */
    public boolean requestLock(PageId pageId, TransactionId tid, int lockType) {
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
     * 添加事务到等待队列
     */
    private void addWaitingTransaction(PageId pageId, TransactionId tid) {
        waitingMap.computeIfAbsent(pageId, k -> new ArrayList<>());
        List<TransactionId> waitingList = waitingMap.get(pageId);
        if (!waitingList.contains(tid)) {
            waitingList.add(tid);
        }
    }

    /**
     * 从等待队列中移除事务
     */
    private void removeWaitingTransaction(PageId pageId, TransactionId tid) {
        if (waitingMap.containsKey(pageId)) {
            List<TransactionId> waitingList = waitingMap.get(pageId);
            waitingList.remove(tid);
            if (waitingList.isEmpty()) {
                waitingMap.remove(pageId);
            }
        }
    }

    /**
     * 释放当前事务在指定页面上的锁。
     *
     * @param pageId
     * @param tid
     */
    public void releaseLock(PageId pageId, TransactionId tid) {
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
     * 移除事务中的所有锁。
     *
     * @param tid
     */
    public synchronized void releaseLocks(TransactionId tid) {
        // 遍历锁映射表，检查每个页面的锁
        for (PageId pageId : lockMap.keySet()) {
            // 调用 releaseLock 方法释放事务在该页面上的锁
            releaseLock(pageId, tid);
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

    /**
     * 检测死锁：利用当前 waitingMap 和 lockMap 构造等待图，
     * 并从给定事务出发做 DFS，检查是否能形成循环。
     *
     * @param startTid 起始事务 ID，即当前正在等待的事务
     * @return 如果检测到死锁则返回 true，否则返回 false
     */
    public synchronized boolean detectDeadlock(TransactionId startTid) {
        // 构造等待图：图中每个节点为事务；边的含义是“等待关系”，即一个事务在等待另一个事务释放它所持有的锁
        // 对于每个页面，在 waitingMap 中记录的每个等待事务都等待着持有该页面锁的事务
        Map<TransactionId, List<TransactionId>> waitForGraph = new ConcurrentHashMap<>();

        // 遍历每个页面上正在等待获取锁的事务列表
        for (Map.Entry<PageId, List<TransactionId>> entry : waitingMap.entrySet()) {
            PageId pageId = entry.getKey();
            List<TransactionId> waitingList = entry.getValue();

            // 获取该页面上所有的锁
            List<Lock> locks = lockMap.get(pageId);
            if (locks == null) continue; // 如果页面上无锁，则跳过该页面

            // 根据锁，获取持有该页面锁的事务列表
            List<TransactionId> holdingTids = new ArrayList<>();
            for (Lock lock : locks) {
                holdingTids.add(lock.getTid());
            }

            // 对于等待该页面锁的每个事务，添加边 waitingTid -> holdingTid
            for (TransactionId waitingTid : waitingList) {
                waitForGraph.computeIfAbsent(waitingTid, k -> new ArrayList<>())
                        .addAll(holdingTids);
            }
        }

        // 使用 BFS 拓扑排序检测死锁
        return bfsDetectCycle(waitForGraph);

        // DFS 方法
//        return dfsHasCycle(waitForGraph, startTid, startTid, new HashSet<>());
    }

    // 使用 BFS 检测环
    private boolean bfsDetectCycle(Map<TransactionId, List<TransactionId>> graph) {
        // 图中的总结点数
        int sum = graph.size();

        // 记录每个事务的入度
        Map<TransactionId, Integer> inDegrees = new HashMap<>();
        for (Map.Entry<TransactionId, List<TransactionId>> entry : graph.entrySet()) {
            TransactionId tid = entry.getKey();
            List<TransactionId> list = entry.getValue();
            inDegrees.put(tid, list.size());
        }

        // BFS 队列，并将入度为 0 的事务加入队列
        Queue<TransactionId> queue = new LinkedList<>();
        for (TransactionId tid : graph.keySet()) {
            if (inDegrees.get(tid) == 0) {
                queue.add(tid);
            }
        }

        // BFS
        while (!queue.isEmpty()) {
            // 取出入度为 0 的事务
            TransactionId pre = queue.poll();
            // 每处理一个节点，计数-1
            sum--;
            // 遍历当前事务邻接表
            for (TransactionId cur : graph.get(pre)) {
                // 减少依赖当前事务的事务的入度
                inDegrees.put(cur, inDegrees.get(cur) - 1);
                // 如果入度为 0，将其加入队列
                if (inDegrees.get(cur) == 0) {
                    queue.add(cur);
                }
            }
        }

        return sum != 0;
    }

    /**
     * 辅助方法：利用 DFS 检测等待图中是否存在环
     *
     * @param graph   等待图，key：事务，value：该事务等待的事务列表
     * @param current 当前遍历的事务
     * @param target  起始事务（待检测环是否能回到此处）
     * @param visited 记录已访问的事务，避免重复遍历
     * @return 如果检测到环则返回 true，否则返回 false
     */
    private boolean dfsHasCycle(Map<TransactionId, List<TransactionId>> graph,
                                TransactionId current,
                                TransactionId target,
                                Set<TransactionId> visited) {
        // 如果当前事务已访问，跳过
        if (visited.contains(current)) {
            return false;
        }
        // 标记当前事务已访问
        visited.add(current);

        // 获取等待当前事务的所有其他事务
        List<TransactionId> neighbors = graph.get(current);
        if (neighbors != null) {
            for (TransactionId neighbor : neighbors) {
                // 如果某个等待的事务就是目标事务 target，
                // 则说明存在一条路径回到了起始事务，形成环（死锁）
                if (neighbor.equals(target)) {
                    return true;
                }
                // 递归搜索
                if (dfsHasCycle(graph, neighbor, target, visited)) {
                    return true;
                }
            }
        }
        return false;
    }
}
