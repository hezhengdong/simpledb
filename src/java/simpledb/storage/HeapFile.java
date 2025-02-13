package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    // 从磁盘文件读取指定页面，并返回对应 Page 对象
    public Page readPage(PageId pid) {
        try {
            // 获取文件的随机访问权限
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            // 计算页面的偏移量
            int pageSize = BufferPool.getPageSize();
            int offset = pid.getPageNumber() * pageSize;
            // 设置文件指针到偏移量
            raf.seek(offset);
            // 读取页面的数据并封装为 HeapPage
            byte[] data = new byte[pageSize];
            // 读取完整的页面数据
            raf.readFully(data);
            // 返回 HeapPage 对象
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.floor(file.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int currentPageIndex = 0;
            private Iterator<Tuple> currentTupleIterator = null;
            private HeapPage currentPage = null;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                // 打开迭代器时，初始化页面迭代
                currentPageIndex = 0;
                loadNextPage();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (currentTupleIterator == null) {
                    return false;
                }

                // 如果当前页面有下一个元组，返回 true
                if (currentTupleIterator.hasNext()) {
                    return true;
                } else {
                    // 否则，加载下一个页面并检查
                    loadNextPage();
                    return currentTupleIterator != null && currentTupleIterator.hasNext();
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (hasNext()) {
                    return currentTupleIterator.next();
                } else {
                    throw new NoSuchElementException("No more tuples in the HeapFile");
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                // 重置当前页面索引为 0
                currentPageIndex = 0;
                // 清空当前元组迭代器
                currentTupleIterator = null;
                // 重新加载第一页
                loadNextPage();
            }

            @Override
            public void close() {
                // 清理资源
                currentPage = null;
                currentTupleIterator = null;
            }

            private void loadNextPage() throws DbException, TransactionAbortedException {
                if (currentPageIndex >= numPages()) {
                    currentTupleIterator = null;
                    return;
                }

                // 使用 BufferPool 获取下一页面
                HeapPageId pid = new HeapPageId(getId(), currentPageIndex);
                currentPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                currentTupleIterator = currentPage.iterator();
                currentPageIndex++;
            }
        };
    }

}

