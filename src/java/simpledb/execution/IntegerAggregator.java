package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbField;      // 分组字段的索引（如果没有分组，则使用 NO_GROUPING）
    private final Type gbFieldType; // 分组字段的类型（如果没有分组，则为 null）
    private final int aField;       // 要聚合的字段的索引（在 Tuple 中的字段位置）
    private final Op op;            // 指定的聚合操作（如 MAX 等）

    // 存储分组结果，key是分组值，value是聚合状态
    private final Map<Field, AggState> groups = new HashMap<>();

    // 辅助类，记录每个分组的聚合状态
    private static class AggState {
        int count = 0;  // 记录数量（用于COUNT/AVG）
        int sum = 0;    // 记录总和（用于SUM/AVG）
        int value;      // 记录当前值（用于MIN/MAX）

        AggState(int firstVal) {
            update(firstVal);
        }

        void update(int val) {
            count++;
            sum += val;
            value = val; // 初始值
        }
    }

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = what;
    }

    /**
     * 核心方法，负责将一个新的 Tuple 合并到现有的聚合结果中
     * <p>
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // 步骤1：获取分组键
        Field groupKey = getGroupKey(tup);

        // 步骤2：获取聚合字段值
        int value = ((IntField) tup.getField(aField)).getValue();

        // 步骤3：更新分组状态
        if (!groups.containsKey(groupKey)) {
            groups.put(groupKey, new AggState(value));
        } else {
            updateGroupState(groups.get(groupKey), value);
        }
    }

    /**
     * 确定当前元组属于哪个分组
     * @param tup
     * @return
     */
    private Field getGroupKey(Tuple tup) {
        if (gbField == NO_GROUPING) {
            return new IntField(-1); // 无分组时使用虚拟键
        }
        return tup.getField(gbField); // 直接使用元组的字段值作为键
    }

    /**
     * 根据聚合操作更新分组的聚合操作
     * @param state
     * @param newVal
     */
    private void updateGroupState(AggState state, int newVal) {
        switch (op) {
            case MIN:
                state.value = Math.min(state.value, newVal);
                break;
            case MAX:
                state.value = Math.max(state.value, newVal);
                break;
            case COUNT:
                state.count++; // 只需要增加计数
                break;
            default: // SUM/AVG
                state.sum += newVal;
                state.count++;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // 步骤1：从 groups 中收集所有分组键并排序
        List<Field> keys = new ArrayList<>(groups.keySet());
        keys.sort(this::compareFields);

        // 步骤2：生成 TupleDesc
        TupleDesc td = buildTupleDesc();

        // 步骤3：生成结果元组
        List<Tuple> tuples = new ArrayList<>();
        // 遍历分组键
        for (Field key : keys) {
            // 获取当前分组的聚合状态
            AggState state = groups.get(key);
            // 为该分组创建结果元组，并将其添加到元组列表中
            tuples.add(createResultTuple(td, key, state));
        }

        // TupleIterator 按照顺序迭代每个分组的聚合结果
        return new TupleIterator(td, tuples);
    }

    /**
     * 比较字段值用于排序
     * @param a
     * @param b
     * @return
     */
    private int compareFields(Field a, Field b) {
        if (a instanceof IntField) {
            return Integer.compare(((IntField) a).getValue(), ((IntField) b).getValue());
        }
        return ((StringField) a).getValue().compareTo(((StringField) b).getValue());
    }

    /**
     * 根据是否有分组字段，创建并返回 TupleDesc（元组描述）。
     * 如果没有分组字段，返回一个只包含聚合结果的描述；
     * 如果有分组字段，返回包含两个字段的描述（一个是分组键，另一个是聚合结果）。
     * @return
     */
    private TupleDesc buildTupleDesc() {
        if (gbField == NO_GROUPING) {
            return new TupleDesc(new Type[]{Type.INT_TYPE});
        }
        return new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE});
    }

    /**
     * 根据提供的 TupleDesc（元组描述）、分组键和聚合状态创建一个新的 Tuple。
     * 该方法会根据聚合结果（如 SUM、MAX）填充 Tuple，并将其返回。
     * @param td
     * @param key
     * @param state
     * @return
     */
    private Tuple createResultTuple(TupleDesc td, Field key, AggState state) {
        Tuple tuple = new Tuple(td);
        int result = calculateResult(state);

        if (gbField == NO_GROUPING) {
            tuple.setField(0, new IntField(result));
        } else {
            tuple.setField(0, key);
            tuple.setField(1, new IntField(result));
        }
        return tuple;
    }

    /**
     * 根据聚合操作类型计算并返回最终结果：
     * @param state
     * @return
     */
    private int calculateResult(AggState state) {
        switch (op) {
            case COUNT: return state.count;
            case SUM: return state.sum;
            case AVG: return state.sum / state.count; // 整数除法
            case MIN: return state.value;
            case MAX: return state.value;
            default: throw new IllegalArgumentException();
        }
    }

}
