package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    
    // 存储分组计数
    private final Map<Field, Integer> groups;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator only supports COUNT");
        }
        
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groups = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (what != Op.COUNT) {
            throw new UnsupportedOperationException("Unsupported aggregation operator");
        }

        Field groupKey = getGroupKey(tup);
        groups.put(groupKey, groups.getOrDefault(groupKey, 0) + 1);
    }

    private Field getGroupKey(Tuple tup) {
        if (gbfield == NO_GROUPING) {
            return new IntField(-1); // 无分组时使用虚拟键
        }
        return tup.getField(gbfield);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // TupleDesc
        TupleDesc td = buildTupleDesc();

        // Tuples
        List<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry : groups.entrySet()) {
            Tuple tuple = new Tuple(td);
            int result = entry.getValue();

            if (gbfield == NO_GROUPING) {
                tuple.setField(0, new IntField(result));
            } else {
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(result));
            }
            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }

    private TupleDesc buildTupleDesc() {
        if (gbfield == NO_GROUPING) {
            return new TupleDesc(new Type[]{Type.INT_TYPE});
        }
        return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
    }

}
