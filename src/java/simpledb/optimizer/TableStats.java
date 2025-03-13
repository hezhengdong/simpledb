package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats 表示查询中基表的统计信息（例如直方图）。
 * <p>
 * TableStats represents statistics (e.g., histograms) about base tables in a 
 * query. 
 * <p>
 * 在实现实验1和实验2时不需要此类。
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    /**
     * 统计信息的并发映射表
     * <br>
     * Concurrent map of table statistics
     */
    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    /**
     * 每页IO成本常数
     * <br>
     * I/O cost per page constant
     */
    static final int IOCOSTPERPAGE = 1000;

    /**
     * 获取指定表的统计信息
     * <br>
     * Get table statistics for specified table
     * 
     * @param tablename 表名<br>
     *                  The name of the table
     * @return 该表的TableStats对象<br>
     *         The TableStats object for the table
     */
    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    /**
     * 设置表的统计信息
     * <br>
     * Set table statistics
     * 
     * @param tablename 表名<br>
     *                  The name of the table
     * @param stats     统计信息对象<br>
     *                  The TableStats object
     */
    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    /**
     * 设置统计信息映射表（反射实现）
     * <br>
     * Set statistics map using reflection
     * 
     * @param s 新的统计信息映射表<br>
     *          New statistics map
     */
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    /**
     * 获取当前统计信息映射表
     * <br>
     * Get current statistics map
     * 
     * @return 统计信息映射表<br>
     *         Current statistics map
     */
    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    /**
     * TableStats 生命周期的开端。<br>
     * 静态方法，在系统启动时被调用，为数据库中的每个表创建一个 TableStats 对象。<br>
     * Compute statistics across all tables<br>
     * 计算所有表的统计信息。
     */
    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * 直方图的桶数量（至少100个）
     * <br>
     * Number of bins for the histogram (at least 100)
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * 添加成员变量
     */
    private final int ioCostPerPage; // 每页 IO 成本
    private final int numTuples;      // 总元组数
    private final int numPages;       // 总页数
    private final TupleDesc tupleDesc; // 表的元组描述信息
    private final Map<Integer, IntHistogram> intHistograms; // 整型字段直方图
    private final Map<Integer, StringHistogram> stringHistograms; // 字符串字段直方图

    /**
     * 创建新的表统计对象
     * <br>
     * Create a new TableStats object
     * 
     * @param tableid        表ID<br>
     *                      The table ID
     * @param ioCostPerPage 每页IO成本<br>
     *                      The cost per page of IO
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // 对于此函数，你需要获取相关表的DbFile，
        // 然后扫描其元组并计算所需的值。
        // 你应该尽量高效地完成，但不必强制
        // （例如）在单次扫描中完成所有操作。
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here

        // 获取 DbFile，除 numsPages 都可以初始化
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.tupleDesc = heapFile.getTupleDesc();
        this.ioCostPerPage = ioCostPerPage;
        this.numPages = heapFile.numPages();
        this.intHistograms = new HashMap<>();
        this.stringHistograms = new HashMap<>();

        // 第一次扫描：收集字段的min/max值
        Map<Integer, Integer> mins = new HashMap<>(); // 记录每个字段的最小值
        Map<Integer, Integer> maxs = new HashMap<>(); // 记录每个字段的最大值
        Transaction t = new Transaction();
        t.start();
        TransactionId tid = t.getId(); // 创建事务ID
        SeqScan scan = new SeqScan(tid, tableid); // 创建顺序扫描
        try {
            scan.open(); // 打开扫描器
            while (scan.hasNext()) {
                Tuple tuple = scan.next(); // 获取下一个元组
                for (int i = 0; i < tupleDesc.numFields(); i++) { // 遍历所有字段
                    if (tupleDesc.getFieldType(i) == Type.INT_TYPE) { // 如果是整型字段
                        int value = ((IntField) tuple.getField(i)).getValue(); // 获取字段值
                        mins.put(i, Math.min(mins.getOrDefault(i, Integer.MAX_VALUE), value)); // 更新最小值
                        maxs.put(i, Math.max(maxs.getOrDefault(i, Integer.MIN_VALUE), value)); // 更新最大值
                    }
                }
            }
            scan.close(); // 关闭扫描器
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 初始化直方图
        for (int i = 0; i < tupleDesc.numFields(); i++) { // 遍历所有字段
            if (tupleDesc.getFieldType(i) == Type.INT_TYPE) { // 如果是整型字段
                int min = mins.getOrDefault(i, 0); // 获取该字段的最小值
                int max = maxs.getOrDefault(i, 0); // 获取该字段的最大值
                intHistograms.put(i, new IntHistogram(NUM_HIST_BINS, min, max)); // 创建直方图并保存
            } else {
                stringHistograms.put(i, new StringHistogram(NUM_HIST_BINS)); // 对字符串类型字段创建直方图
            }
        }

        // 第二次扫描：填充直方图数据
        int tupleCount = 0; // 记录元组数量
        try {
            scan.open(); // 打开扫描器
            while (scan.hasNext()) {
                Tuple tuple = scan.next(); // 获取下一个元组
                tupleCount++; // 增加元组计数
                for (int i = 0; i < tupleDesc.numFields(); i++) { // 遍历所有字段
                    Field field = tuple.getField(i); // 获取字段
                    if (field.getType() == Type.INT_TYPE) { // 如果是整型字段
                        intHistograms.get(i).addValue(((IntField) field).getValue()); // 添加到直方图
                    } else {
                        stringHistograms.get(i).addValue(((StringField) field).getValue()); // 对于字符串字段，添加到字符串直方图
                    }
                }
            }
            scan.close();
            t.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.numTuples = tupleCount; // 设置总元组数
    }

    /**
     * 估计顺序扫描成本
     * <br>
     * Estimate the cost of sequential scan
     * 
     * @return 估计的扫描成本<br>
     *         Estimated scan cost
     */
    public double estimateScanCost() {
        // some code goes here
        // 计算扫描成本 = 页数 * 每页IO成本
        return numPages * ioCostPerPage;
    }

    /**
     * 估计表基数（考虑选择率）
     * <br>
     * Estimate table cardinality with selectivity
     * 
     * @param selectivityFactor 选择率因子<br>
     *                          The selectivity factor
     * @return 估计的基数<br>
     *         Estimated cardinality
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        // 基数估计 = 总元组数 * 选择率
        return (int) Math.round(numTuples * selectivityFactor);
    }

    /**
     * 获取字段的平均选择率
     * <br>
     * Get average selectivity for field
     * 
     * @param field 字段索引<br>
     *              Field index
     * @param op    操作符<br>
     *              Operator
     * @return 平均选择率<br>
     *         Average selectivity
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        // 默认返回1.0，代表完全选择性
        return 1.0;
    }

    /**
     * 估计谓词选择率
     * <br>
     * Estimate predicate selectivity
     * 
     * @param field     字段索引<br>
     *                  Field index
     * @param op        操作符<br>
     *                  Operator
     * @param constant  比较常量<br>
     *                  Comparison constant
     * @return 估计的选择率<br>
     *         Estimated selectivity
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        // 如果是整型字段，使用整型直方图进行估算
        if (tupleDesc.getFieldType(field) == Type.INT_TYPE) {
            IntHistogram hist = intHistograms.get(field);
            int value = ((IntField) constant).getValue();
            return hist.estimateSelectivity(op, value);
        } else { // 如果是字符串字段，使用字符串直方图进行估算
            StringHistogram hist = stringHistograms.get(field);
            String value = ((StringField) constant).getValue();
            return hist.estimateSelectivity(op, value);
        }
    }

    /**
     * 获取总元组数
     * <br>
     * Get total number of tuples
     * 
     * @return 总元组数<br>
     *         Total number of tuples
     */
    public int totalTuples() {
        // some code goes here
        return numTuples;
    }
}
