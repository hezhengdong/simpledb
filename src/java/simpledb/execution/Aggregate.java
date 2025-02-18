package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

import static simpledb.execution.Aggregator.NO_GROUPING;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;        // 输入数据的迭代器，即聚合操作的源数据
    private final int afield;        // 要聚合的字段在输入元组中的索引
    private final int gfield;        // 分组字段的索引（如果没有分组，则为 NO_GROUPING）
    private final Aggregator.Op aop; // 聚合操作类型（如 SUM）
    private Aggregator aggregator;   // 聚合器对象（如 IntegerAggregator）
    private OpIterator aggIterator;  // 聚合结果的迭代器，遍历聚合后的结果
    private TupleDesc td;            // 结果元组的描述，定义聚合结果的字段和类型

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        
        // 确定分组字段类型
        Type gbtype = gfield == NO_GROUPING ? null : child.getTupleDesc().getFieldType(gfield);

        // 根据字段类型选择聚合器
        Type afieldType = child.getTupleDesc().getFieldType(afield);
        if (afieldType == Type.STRING_TYPE) {
            if (aop != Aggregator.Op.COUNT) {
                throw new IllegalArgumentException("String aggregation only supports COUNT");
            }
            this.aggregator = new StringAggregator(gfield, gbtype, afield, aop);
        } else {
            this.aggregator = new IntegerAggregator(gfield, gbtype, afield, aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        return gfield == NO_GROUPING ? null : child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    /**
     * 将聚合操作符转换为字符串
     * @param aop
     * @return
     */
    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open();
        child.open();
        
        // 处理所有元组，执行聚合操作
        while (child.hasNext()) {
            Tuple t = child.next();
            aggregator.mergeTupleIntoGroup(t);
        }
        child.close();

        // 获取并打开聚合结果的迭代器
        aggIterator = aggregator.iterator();
        aggIterator.open();
    }

    /**
     * 返回聚合结果中的下一个元组。如果聚合结果的迭代器有下一个元组，则返回它；否则返回 null。
     * <p>
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (aggIterator != null && aggIterator.hasNext()) {
            return aggIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        aggIterator.rewind();
    }

    /**
     * 构建并返回结果元组的描述。根据是否有分组字段，元组描述会有所不同：
     * <p>
     * - 如果没有分组字段，仅包含一个聚合结果字段。
     * <p>
     * - 如果有分组字段，则包含分组字段和聚合结果字段。
     * <p>
     * 结果字段的名称会包括聚合操作符和聚合字段名称，例如 SUM(field_name)。
     * <p>
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        if (td == null) {
            // 构建结果元组描述
            if (gfield == NO_GROUPING) {
                td = new TupleDesc(new Type[]{Type.INT_TYPE},
                        new String[]{String.format("%s(%s)", aop, aggregateFieldName())});
            } else {
                td = new TupleDesc(
                        new Type[]{child.getTupleDesc().getFieldType(gfield), Type.INT_TYPE},
                        new String[]{
                                groupFieldName(),
                                String.format("%s(%s)", aop, aggregateFieldName())
                        });
            }
        }
        return td;
    }

    public void close() {
        super.close();
        aggIterator.close();
        aggIterator = null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length != 1) {
            throw new IllegalArgumentException("Aggregate expects only one child");
        }
        this.child = children[0];
    }

}
