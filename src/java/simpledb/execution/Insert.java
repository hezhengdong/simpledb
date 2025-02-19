package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private OpIterator child;
    private final int tableId;
    private boolean inserted;
    private final TupleDesc resultTd;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // 验证元组结构是否匹配
        TupleDesc tableTd = Database.getCatalog().getTupleDesc(tableId);
        if (!child.getTupleDesc().equals(tableTd)) {
            throw new DbException("Tuple desc mismatch");
        }
        
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.inserted = false;
        this.resultTd = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        return resultTd;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (inserted) {
            return null; // 只返回一次结果
        }

        // 逐个添加数据源中的元组
        int count = 0;
        try {
            while (child.hasNext()) {
                Tuple t = child.next();
                Database.getBufferPool().insertTuple(tid, tableId, t);
                count++;
            }
        } catch (IOException e) {
            throw new DbException("IO error during insertion");
        }
        
        // 构建结果元组
        Tuple result = new Tuple(resultTd);
        result.setField(0, new IntField(count));
        inserted = true;
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length != 1) {
            throw new IllegalArgumentException("Insert requires exactly one child");
        }
        child = children[0];
    }
}
