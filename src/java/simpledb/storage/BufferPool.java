package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedHashMap;
import java.util.Iterator;

/**
 * BufferPool 负责管理从磁盘向内存读写页面。
 * <p>
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages; // BufferPool 中可缓存的最大页面数

    // 存储具体页面
    private final ConcurrentHashMap<PageId, Page> pageStore = new ConcurrentHashMap<>();
    // 存储页面的访问顺序，键为 PageId，值无实际意义
    private final LinkedHashMap<PageId, Boolean> lruCache = new LinkedHashMap<>(16, 0.75f, true);

    private LockManager lockManager = new LockManager();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
    }

    public static int getPageSize() {
      return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * 获取指定页面，并更新 LRU 缓存中的访问顺序
     * <p>
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

        // 获取锁权限
        int lockType;
        if (perm == Permissions.READ_ONLY) {
            lockType = Permissions.READ_ONLY.ordinal();
        } else {
            lockType = Permissions.READ_WRITE.ordinal();
        }

        // 设置超时时间（1秒）
        long timeout = 1000;
        long startTime = System.currentTimeMillis();
        // 尝试为页面设置锁
        while (!lockManager.setLock(pid, tid, lockType)) {
            // 检查是否超过了超时时间
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (elapsedTime > timeout) {
                // 如果超过超时时间，抛出异常
                throw new TransactionAbortedException();
            }
        }

        // 如果页面存在
        if (lruCache.containsKey(pid)) {
            // 更新 LRU 缓存
            lruCache.get(pid);
            return pageStore.get(pid);
        }

        // 如果缓冲池已满，进行页面淘汰
        if (pageStore.size() >= numPages) {
            evictPage();
        }

        // 如果页面不存在

        // 获取数据库文件并读取新页面
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = dbFile.readPage(pid);

        // 更新缓存
        pageStore.put(pid, page);
        // 新增 LRU 缓存
        lruCache.put(pid, true);

        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(pid, tid);
    }

    /**
     * 释放与特定事务相关的所有锁。<br>
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * 判断给定事务是否持有指定页面的锁。<br>
     * Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(p, tid);
    }

    /**
     * 提交或中止某个事务；<br>
     * 释放与该事务相关的所有锁。<br>
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock<br>
     *            请求解锁的事务 ID
     * @param commit a flag indicating whether we should commit or abort<br>
     *               一个标志，用于指示事务是要 commit（提交） 还是 abort（回滚）。
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            restorePages(tid);
        }
        lockManager.releaseLocks(tid);
    }

    private synchronized void restorePages(TransactionId tid) {
        for (PageId pid : pageStore.keySet()) {
            Page page = pageStore.get(pid);
            lruCache.get(pid);
            // 检查页面是否是脏页，并且是由当前事务修改的
            if (page.isDirty() != null && page.isDirty().equals(tid)) {
                // 恢复页面到修改前的状态
                Page oldPage = page.getBeforeImage();
                pageStore.put(pid, oldPage); // 更新缓存中的页面
                page.markDirty(false, null); // 清除脏页标记
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> curPage = dbFile.insertTuple(tid, t);
        for (Page page : curPage) {
            page.markDirty(true, tid);
            pageStore.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> curPage = dbFile.deleteTuple(tid, t);
        for (Page page : curPage) {
            page.markDirty(true, tid);
            pageStore.put(page.getId(), page);
        }
    }

    /**
     * 刷新所有脏页到磁盘上。<br>
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        // 遍历所有页面
        for (PageId pid : pageStore.keySet()) {
            try {
                flushPage(pid);
            } catch (IOException e) {
                throw new IOException("Failed to flush page " + pid, e);
            }
        }
    }

    /** 
     *  从缓冲池中移除页面而不将其刷新到磁盘（丢弃修改）（Lab2.5要求实现，说后面会用到）
     *  Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        // 从页面存储中移除
        Page removed = pageStore.remove(pid);
        
        // 从LRU缓存中移除
        synchronized (lruCache) {
            lruCache.remove(pid);
        }
        
        // 如果被移除的是脏页，需要清理相关状态
        if (removed != null && removed.isDirty() != null) {
            // 注意：这里不刷盘，直接丢弃修改
            removed.markDirty(false, null); // 清除脏页标记
        }
    }

    /**
     * 刷新指定页到磁盘上。<br>
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        // 获取页面实例
        Page page = pageStore.get(pid);
        if (page == null) {
            return; // 页面不在缓存中
        }

        // 仅处理脏页
        if (page.isDirty() != null) {
            // 获取对应的数据库文件
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            // 写入磁盘
            dbFile.writePage(page);
            // 事务中会用到，作用：保存当前页面状态作为前镜像，以便在事务回滚时，恢复到刷盘前的状态
            page.setBeforeImage();
            // 清除脏页标记，脏页变为了缓冲池中的正常页
            page.markDirty(false, null);
        }
    }

    /**
     * 刷新指定事务的页到磁盘上。（相当于刷新事务中的脏页到磁盘上。）<br>
     * Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : pageStore.keySet()) {
            Page page = pageStore.get(pid);
            if (page.isDirty() != null && page.isDirty().equals(tid)) {
                flushPage(pid);
            }
        }
    }

    /**
     * 从缓冲池中丢弃一个页面。
     * <p>
     * 将页面刷新到磁盘，确保脏页面在磁盘上得到更新。
     * <p>
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        PageId removePid = null;

        // 遍历 LRU 缓存，找到最久未使用的页面
        for (PageId pid : lruCache.keySet()) {
            Page page = pageStore.get(pid);
            // 如果页面未修改，不是脏页，则可以淘汰
            if (page.isDirty() == null) {
                removePid = pid;
                break;
            }
        }

        // 如果找到了可以淘汰的、非脏页的页面，则移除。
        if (removePid != null) {
            lruCache.remove(removePid);
            pageStore.remove(removePid);
        }
        // 如果没有找到可以淘汰的页面，说明缓冲池中剩下的都是脏页，开始淘汰脏页。
        else {
            // 获取脏页 ID，即 lruCache 的第一个元素
            for (PageId pid : lruCache.keySet()) {
                removePid = pid;
                break;
            }
            // 刷新脏页到磁盘上
            try {
                flushPage(removePid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            // 从缓冲池和 LRU 中移除脏页
            lruCache.remove(removePid);
            pageStore.remove(removePid);
        }
    }

}
