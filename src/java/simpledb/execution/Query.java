package simpledb.execution;

import simpledb.optimizer.LogicalPlan;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.io.*;
import java.util.*;

/**
 * Query is a wrapper class to manage the execution of queries. It takes a query
 * plan in the form of a high level OpIterator (built by initiating the
 * constructors of query plans) and runs it as a part of a specified
 * transaction.
 * 
 * @author Sam Madden
 */

public class Query implements Serializable {

    private static final long serialVersionUID = 1L;

    transient private OpIterator op; // 表示查询的物理执行计划，由 LogicalPlan.physicalPlan() 方法生成。
    transient private LogicalPlan logicalPlan; // 表示查询的逻辑计划，包含 SQL 查询的结构信息。
    final TransactionId tid; // 表示执行查询的事务ID。
    transient private boolean started = false; // 跟踪查询是否已经开始执行。

    public TransactionId getTransactionId() {
        return this.tid;
    }

    public void setLogicalPlan(LogicalPlan lp) {
        this.logicalPlan = lp;
    }

    public LogicalPlan getLogicalPlan() {
        return this.logicalPlan;
    }

    public void setPhysicalPlan(OpIterator pp) {
        this.op = pp;
    }

    public OpIterator getPhysicalPlan() {
        return this.op;
    }

    public Query(TransactionId t) {
        tid = t;
    }

    public Query(OpIterator root, TransactionId t) {
        op = root;
        tid = t;
    }

    public void start() throws DbException,
            TransactionAbortedException {
        op.open();

        started = true;
    }

    public TupleDesc getOutputTupleDesc() {
        return this.op.getTupleDesc();
    }

    /** @return true if there are more tuples remaining. */
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return op.hasNext();
    }

    /**
     * Returns the next tuple, or throws NoSuchElementException if the iterator
     * is closed.
     * 
     * @return The next tuple in the iterator
     * @throws DbException
     *             If there is an error in the database system
     * @throws NoSuchElementException
     *             If the iterator has finished iterating
     * @throws TransactionAbortedException
     *             If the transaction is aborted (e.g., due to a deadlock)
     */
    public Tuple next() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        if (!started)
            throw new DbException("Database not started.");

        return op.next();
    }

    /** Close the iterator */
    public void close() {
        op.close();
        started = false;
    }

    /**
     * 核心方法，完整执行查询并输出结果
     *
     * @throws DbException
     * @throws TransactionAbortedException
     */
    public void execute() throws DbException, TransactionAbortedException {
        // 获取输出元组的描述(TupleDesc)
        TupleDesc td = this.getOutputTupleDesc();

        // 打印表头（字段名称）
        StringBuilder names = new StringBuilder();
        for (int i = 0; i < td.numFields(); i++) {
            names.append(td.getFieldName(i)).append("\t");
        }
        System.out.println(names);

        // 打印分隔线以提高可读性
        for (int i = 0; i < names.length() + td.numFields() * 4; i++) {
            System.out.print("-");
        }
        System.out.println();

        this.start();
        int cnt = 0;
        // 循环调用hasNext()和next()遍历所有结果元组，并打印
        while (this.hasNext()) {
            Tuple tup = this.next();
            System.out.println(tup);
            cnt++;
        }
        // 统计并打印结果行数。
        System.out.println("\n " + cnt + " rows.");
        this.close();
    }
}
