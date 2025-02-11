package simpledb.storage;

import java.util.Objects;

/**
 * Unique identifier for HeapPage objects.
 * <p>
 * HeapPage 对象的唯一标识符
 */
public class HeapPageId implements PageId {

    private final int tableId;

    private final int pgNo;

    /**
     * 构造函数。为特定表格的特定页面创建一个
     * <p>
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
        this.tableId = tableId;
        this.pgNo = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // some code goes here
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        // some code goes here
        return pgNo;
    }

    /**
     * equal 为 true，hashCode 必然为 true
     * <p>
     * equal 为 false，hashCode 大概率为 false，小概率为 true
     * <p>
     * Hash 算法，相同输入得到相同输出，不同输入得到不同输出
     * @return a hash code for this page, represented by a combination of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here
        return Objects.hash(tableId, pgNo);
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
        // 原理见 https://liaoxuefeng.com/books/java/collection/equals/index.html
        if (o instanceof PageId pi) {
            return Objects.equals(this.tableId, pi.getTableId()) && Objects.equals(this.pgNo, pi.getPageNumber());
        }
        return false;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int[] data = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
