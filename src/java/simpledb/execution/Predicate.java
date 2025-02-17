package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 比较操作符定义
     * <p>
     * Constants used for return codes in Field.compare
     */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * 通过序号获取操作符
         * <p>
         * 为什么需要这个方法？
         * <p>
         * 答：数据库系统需要将操作符序列化存储（比如保存查询计划时）
         * <p>
         * Interface to access operations by integer value for command-line
         * convenience.
         * 
         * @param i
         *            a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }


        /**
         * 符号转换方法
         * <p>
         * 为什么需要toString？
         * <p>
         * 答：用于查询计划的可视化显示（如EXPLAIN命令）
         * @return
         */
        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }

    // 谓词三要素
    private final int field;     // 要比较的字段位置（age是第几个字段？）
    private final Op op;         // 比较操作符（> 还是 =）
    private final Field operand; // 比较值

    /**
     * Constructor.
     * 
     * @param field
     *            field number of passed in tuples to compare against.
     * @param op
     *            operation to use for comparison
     * @param operand
     *            field value to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
        // some code goes here
        this.field = field;
        this.op = op;
        this.operand = operand;
    }

    /**
     * @return the field number
     */
    public int getField()
    {
        // some code goes here
        return field;
    }

    /**
     * @return the operator
     */
    public Op getOp()
    {
        // some code goes here
        return op;
    }
    
    /**
     * @return the operand
     */
    public Field getOperand()
    {
        // some code goes here
        return operand;
    }
    
    /**
     * 核心过滤方法
     * <p>
     * 翻译：使用构造函数中指定的运算符，将构造函数中指定的 t 的字段编号与构造函数中指定的操作数字段进行比较。可以通过 Field 的比较方法进行比较。
     * <p>
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     * 
     * @param t
     *            The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        // some code goes here
        // 为什么需要getField(field)？
        // 答：从元组中提取要比较的字段值（如获取age的值）
        Field fieldValue = t.getField(field);

        // 为什么需要compare方法？
        // 答：不同类型的字段（Int/String）需要实现自己的比较逻辑
        return fieldValue.compare(op, operand);
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
        // some code goes here
        return String.format("f = %d op = %s operand = %s", field,op.toString(),operand.toString());
    }
}
