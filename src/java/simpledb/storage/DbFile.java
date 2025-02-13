
package simpledb.storage;

import simpledb.common.DbException;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * 数据库文件在磁盘上的接口。
 * <p>
 * 每个表对应一个 DbFile。DbFile 可以获取页（Page）并遍历元组（Tuple）。
 * <p>
 * 每个文件有一个唯一的 id，用于在 Catalog 中存储表的元数据。
 * <p>
 * 操作符一般通过缓冲池（BufferPool）访问 DbFile，而不是直接操作。
 * <p>
 * The interface for database files on disk. Each table is represented by a
 * single DbFile. DbFiles can fetch pages and iterate through tuples. Each
 * file has a unique id used to store metadata about the table in the Catalog.
 * DbFiles are generally accessed through the buffer pool, rather than directly
 * by operators.
 */
public interface DbFile {
    /**
     * 从磁盘读取指定页。
     * <p>
     * Read the specified page from disk.
     *
     * @throws IllegalArgumentException if the page does not exist in this file.
     */
    Page readPage(PageId id);

    /**
     * 将指定页写入磁盘。
     * <p>
     * Push the specified page to disk.
     *
     * @param p The page to write.  page.getId().pageno() specifies the offset into the file where the page should be written.
     *          <p>要写入的页。page.getId().pageno() 指定了页在文件中的写入偏移量。
     * @throws IOException if the write fails
     *
     */
    void writePage(Page p) throws IOException;

    /**
     * 代表事务将指定元组插入文件。
     * <p>
     * 此方法会获取文件受影响页的锁，可能会阻塞直到锁被获取。
     * <p>
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update 执行更新的交易
     * @param t The tuple to add.  This tuple should be updated to reflect that
     *          it is now stored in this file. 要添加的元组。该元组应更新为反映其已存储在此文件中。
     * @return An ArrayList contain the pages that were modified 包含被修改页的 ArrayList
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    List<Page> insertTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException;

    /**
     * 代表事务从文件中删除指定元组。
     * <p>
     * 此方法会获取文件受影响页的锁，可能会阻塞直到锁被获取。
     * <p>
     * Removes the specified tuple from the file on behalf of the specified
     * transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to delete.  This tuple should be updated to reflect that
     *          it is no longer stored on any page.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be deleted or is not a member
     *   of the file
     */
    List<Page> deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException;

    /**
     * 返回遍历此 DbFile 中所有元组的迭代器。
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    DbFileIterator iterator(TransactionId tid);

    /**
     * 返回此 DbFile 在 Catalog 中使用的唯一 ID。
     * <p>
     * 此 ID 可用于通过 {@link Catalog#getDatabaseFile} 和 {@link Catalog#getTupleDesc} 查找表。
     * <p>
     * 实现提示：需在某处生成此 tableid，
     * <p>
     * 确保每个 HeapFile 有 "唯一 id"，且对同一 HeapFile 始终返回相同值。
     * <p>
     * 一种简单实现是使用底层文件的绝对路径的哈希码，
     * <p>
     * 例如 <code>f.getAbsoluteFile().hashCode()</code>。
     * <p>
     * Returns a unique ID used to identify this DbFile in the Catalog. This id
     * can be used to look up the table via {@link Catalog#getDatabaseFile} and
     * {@link Catalog#getTupleDesc}.
     * <p>
     * Implementation note:  you will need to generate this tableid somewhere,
     * ensure that each HeapFile has a "unique id," and that you always
     * return the same value for a particular HeapFile. A simple implementation
     * is to use the hash code of the absolute path of the file underlying
     * the HeapFile, i.e. <code>f.getAbsoluteFile().hashCode()</code>.
     *
     * @return an ID uniquely identifying this HeapFile. 唯一标识此 HeapFile 的 ID。
     */
    int getId();
    
    /**
     * 返回此 DbFile 存储的表的元组描述（TupleDesc）。
     * <p>
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    TupleDesc getTupleDesc();
}
