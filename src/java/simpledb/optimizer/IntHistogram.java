package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * 一个类，用于表示单个整数字段上固定宽度直方图。
 * <p>
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int[] buckets; // 记录选择性
    private final int min, max; // 直方图中数据（横坐标）的最小值与最大值
    private final double width; // 每个桶的宽度，即每个桶所表示的数值范围
    private int sum = 0; // 桶中元素的总数

    /**
     * Create a new IntHistogram.
     * <p>
     * IntHistogram 应维护其接收到的整数值直方图。
     * <p>
     * 它应将直方图分割成 "buckets "桶。
     * <p>
     * 直方图中的值将通过 "addValue() "函数逐个提供。
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     * <br>
     * 您的实现所占用的空间和执行时间应与直方图值的数量保持恒定。例如，不能简单地存储排序列表中的每个值。
     *
     * @param buckets The number of buckets to split the input value into.<br>
     *                将输入值分割成的桶数。
     * @param min The minimum integer value that will ever be passed to this class for histogramming<br>
     *            将传递给该类进行直方图绘制的最小整数值
     * @param max The maximum integer value that will ever be passed to this class for histogramming<br>
     *            将传递给该类进行直方图绘制的最大整数值
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.width = (max - min + 1.0) / buckets;
    }

    public int getIndex(int v) {
        // 如果 v 不在 [min, max] 范围内，忽略该值
        if (v < min || v > max) {
            throw new IllegalArgumentException("参数不合规范");
        }
        // 计算该值属于哪个桶
        int bucketIndex = (int) ((v - min) / width);
        // 防止越界
        if (bucketIndex >= buckets.length) {
            bucketIndex = buckets.length - 1;
        }
        return bucketIndex;
    }

    /**
     * 向直方图中添加一个新的值
     * <p>
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        buckets[getIndex(v)]++;  // 增加对应桶的计数
        sum++;  // 总计数加 1
    }

    /**
     * 估算表中特定谓词和操作数的选择性
     * <p>
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * 例如，如果 op 是 "greater_than"(大于)且 "v" 是 5，则返回大于 5 的元素的估计比例。
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value<br>预测该运算符和数组的选择性
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        // 处理 LESS_THAN 操作符
        switch (op) {
            case LESS_THAN:
                // 如果 v 小于或等于 min，选择性为 0（没有符合条件的元素）
                if (v <= min) return 0.0;
                // 如果 v 大于或等于 max，选择性为 1（所有元素都符合条件）
                if (v >= max) return 1.0;

                // 找到 v 所在桶的索引
                final int index = getIndex(v);
                double cnt = 0.0;

                // 计算所有小于 v 的桶的总计数
                for (int i = 0; i < index; i++) {
                    cnt += buckets[i];
                }

                // 计算 v 所在桶的部分贡献
                cnt += buckets[index] * (v - index * width - min) / width;
                // 返回小于 v 的元素比例，sum 为总元素数
                return cnt / sum;

            case LESS_THAN_OR_EQ:
                // LESS_THAN 计数 + 1
                return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);

            case GREATER_THAN:
                // LESS_THAN_OR_EQ 的反向操作
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);

            case GREATER_THAN_OR_EQ:
                // GREATER_THAN 计数 - 1
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);

            case EQUALS:
                // LESS_THAN_OR_EQ - LESS_THAN
                return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) -
                        estimateSelectivity(Predicate.Op.LESS_THAN, v);

            case NOT_EQUALS:
                // EQUALS - 1
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }

        // 若操作符不在上述情况，默认返回 0.0
        return 0.0;
    }

    /**
     * @return
     *     the average selectivity of this histogram.<br>
     *     直方图的平均选择性。
     *     <p>
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization<br>
     *     这并不是实现基本连接优化必不可少的方法。如果您希望实现更有效的优化，则可能需要它
     * */
    public double avgSelectivity()
    {
        // some code goes here
        // 总元素数
        double totalSelectivity = 0.0;

        // 计算每个桶的选择性并累加
        for (int i = 0; i < buckets.length; i++) {
            totalSelectivity += (double) buckets[i] / sum;
        }

        // 返回平均选择性
        return totalSelectivity / buckets.length;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < buckets.length; i++) {
            sb.append("Bucket " + i + ": " + buckets[i] + " tuples\n");
        }
        return sb.toString();
    }
}
