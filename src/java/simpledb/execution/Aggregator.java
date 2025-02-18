package simpledb.execution;

import simpledb.storage.Tuple;
import simpledb.storage.TupleIterator;

import java.io.Serializable;

/**
 * 用于计算一组 Tuple 对象的聚合操作
 * <p>
 * The common interface for any class that can compute an aggregate over a
 * list of Tuples.
 */
public interface Aggregator extends Serializable {
    // 表示没有进行分组操作
    int NO_GROUPING = -1;

    /**
     * 表示可能的聚合操作
     * <p>
     * SUM_COUNT and SC_AVG will
     * only be used in lab7, you are not required
     * to implement them until then.
     * */
    enum Op implements Serializable {
        MIN,
        MAX,
        SUM,
        AVG, // 平均值
        COUNT, // 计数
        /**
         * SUM_COUNT: compute sum and count simultaneously, will be
         * needed to compute distributed avg in lab7.
         * */
        SUM_COUNT,
        /**
         * SC_AVG: compute the avg of a set of SUM_COUNT tuples,
         * will be used to compute distributed avg in lab7.
         * */
        SC_AVG;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }
        
        public String toString()
        {
        	if (this==MIN)
        		return "min";
        	if (this==MAX)
        		return "max";
        	if (this==SUM)
        		return "sum";
        	if (this==SUM_COUNT)
    			return "sum_count";
        	if (this==AVG)
        		return "avg";
        	if (this==COUNT)
        		return "count";
        	if (this==SC_AVG)
    			return "sc_avg";
        	throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * 用于将新的 Tuple 添加到聚合结果中。如果该组的聚合结果尚未存在，则会创建一个新的聚合结果。
     * <p>
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field.
     *            包含 aggregate 字段和分组字段的元组
     */
    void mergeTupleIntoGroup(Tuple tup);

    /**
     * 迭代所有的聚合结果
     * <p>
     * Create a OpIterator over group aggregate results.
     * @see TupleIterator for a possible helper
     */
    OpIterator iterator();
    
}
