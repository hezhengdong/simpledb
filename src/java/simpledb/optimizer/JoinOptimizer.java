package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.ParsingException;
import simpledb.execution.*;
import simpledb.storage.TupleDesc;

import java.util.*;

import javax.swing.*;
import javax.swing.tree.*;

/**
 * The JoinOptimizer class is responsible for ordering a series of joins
 * optimally, and for selecting the best instantiation of a join for a given
 * logical plan.
 */
public class JoinOptimizer {
    final LogicalPlan p; // 正在优化的逻辑计划
    final List<LogicalJoinNode> joins; // 正在执行的连接列表

    /**
     * Constructor
     *
     * @param p
     *            the logical plan being optimized
     * @param joins
     *            the list of joins being performed
     */
    public JoinOptimizer(LogicalPlan p, List<LogicalJoinNode> joins) {
        this.p = p;
        this.joins = joins;
    }

    /**
     * Return best iterator for computing a given logical join, given the
     * specified statistics, and the provided left and right subplans. Note that
     * there is insufficient information to determine which plan should be the
     * inner/outer here -- because OpIterator's don't provide any cardinality
     * estimates, and stats only has information about the base tables. For this
     * reason, the plan1
     *
     * @param lj
     *            The join being considered
     * @param plan1
     *            The left join node's child
     * @param plan2
     *            The right join node's child
     */
    public static OpIterator instantiateJoin(LogicalJoinNode lj,
                                             OpIterator plan1, OpIterator plan2) throws ParsingException {

        int t1id = 0, t2id = 0;
        OpIterator j;

        try {
            t1id = plan1.getTupleDesc().fieldNameToIndex(lj.f1QuantifiedName);
        } catch (NoSuchElementException e) {
            throw new ParsingException("Unknown field " + lj.f1QuantifiedName);
        }

        if (lj instanceof LogicalSubplanJoinNode) {
            t2id = 0;
        } else {
            try {
                t2id = plan2.getTupleDesc().fieldNameToIndex(
                        lj.f2QuantifiedName);
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown field "
                        + lj.f2QuantifiedName);
            }
        }

        JoinPredicate p = new JoinPredicate(t1id, lj.p, t2id);

        if (lj.p == Predicate.Op.EQUALS) {

            try {
                // dynamically load HashEquiJoin -- if it doesn't exist, just
                // fall back on regular join
                Class<?> c = Class.forName("simpledb.execution.HashEquiJoin");
                java.lang.reflect.Constructor<?> ct = c.getConstructors()[0];
                j = (OpIterator) ct
                        .newInstance(new Object[] { p, plan1, plan2 });
            } catch (Exception e) {
                j = new Join(p, plan1, plan2);
            }
        } else {
            j = new Join(p, plan1, plan2);
        }

        return j;

    }

    /**
     * Estimate the cost of a join.<br>
     * 估算连接操作的成本<br>
     *
     * The cost of the join should be calculated based on the join algorithm (or
     * algorithms) that you implemented for Lab 2. It should be a function of
     * the amount of data that must be read over the course of the query, as
     * well as the number of CPU opertions performed by your join. Assume that
     * the cost of a single predicate application is roughly 1.<br>
     * 连接成本应根据你在Lab 2中实现的连接算法计算。成本应考虑查询过程中需要读取的数据量
     * 以及连接操作执行的CPU操作次数。假设单个谓词应用的成本大约为1。<br>
     *
     * @param j
     *            A LogicalJoinNode representing the join operation being
     *            performed. <br>
     *            表示要执行的连接操作的LogicalJoinNode
     * @param card1
     *            Estimated cardinality of the left-hand side of the query <br>
     *            左查询的估算基数
     * @param card2
     *            Estimated cardinality of the right-hand side of the query <br>
     *            右查询的估算基数
     * @param cost1
     *            Estimated cost of one full scan of the table on the left-hand
     *            side of the query <br>
     *            左表全表扫描的估算成本
     * @param cost2
     *            Estimated cost of one full scan of the table on the right-hand
     *            side of the query <br>
     *            右表全表扫描的估算成本
     * @return An estimate of the cost of this query, in terms of cost1 and
     *         cost2 <br>
     *         基于cost1和cost2的查询成本估算值
     */
    public double estimateJoinCost(LogicalJoinNode j, int card1, int card2,
            double cost1, double cost2) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 3.
            return card1 + cost1 + cost2;
        } else {
            // Insert your code here.
            // HINT: You may need to use the variable "j" if you implemented
            // a join algorithm that's more complicated than a basic
            // nested-loops join.
            return cost1 + card1 * cost2 + card1 * card2;
        }
    }

    /**
     * Estimate the cardinality of a join. The cardinality of a join is the
     * number of tuples produced by the join.<br>
     * 估算连接操作的基数（连接结果产生的元组数量）<br>
     *
     * @param j
     *            A LogicalJoinNode representing the join operation being
     *            performed. <br>
     *            表示要执行的连接操作的LogicalJoinNode
     * @param card1
     *            Cardinality of the left-hand table in the join <br>
     *            左表的基数
     * @param card2
     *            Cardinality of the right-hand table in the join <br>
     *            右表的基数
     * @param t1pkey
     *            Is the left-hand table a primary-key table? <br>
     *            左表是否是主键表
     * @param t2pkey
     *            Is the right-hand table a primary-key table? <br>
     *            右表是否是主键表
     * @param stats
     *            The table stats, referenced by table names, not alias <br>
     *            以表名（非别名）引用的表统计信息
     * @return The cardinality of the join <br>
     *         连接操作的基数
     */
    public int estimateJoinCardinality(LogicalJoinNode j, int card1, int card2,
            boolean t1pkey, boolean t2pkey, Map<String, TableStats> stats) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 3.
            return card1;
        } else {
            return estimateTableJoinCardinality(j.p, j.t1Alias, j.t2Alias,
                    j.f1PureName, j.f2PureName, card1, card2, t1pkey, t2pkey,
                    stats, p.getTableAliasToIdMapping());
        }
    }

    /**
     * Estimate the join cardinality of two tables.
     * */
    public static int estimateTableJoinCardinality(Predicate.Op joinOp,
                                                   String table1Alias, String table2Alias, String field1PureName,
                                                   String field2PureName, int card1, int card2, boolean t1pkey,
                                                   boolean t2pkey, Map<String, TableStats> stats,
                                                   Map<String, Integer> tableAliasToId) {
        int card = 1;
        // some code goes here
        if (joinOp.equals(Predicate.Op.EQUALS)) {
            if (!t1pkey && !t2pkey) {
                card = Math.max(card1, card2);
            } else if (t1pkey && t2pkey) {
                card = Math.min(card1, card2);
            } else if (t1pkey) {
                card = card1;
            } else {
                card = card2;
            }
        } else {
            card = (int) (0.3 * card1 * card2);
        }
        return card <= 0 ? 1 : card;
    }

    /**
     * Helper method to enumerate all of the subsets of a given size of a
     * specified vector.
     *
     * @param v
     *            The vector whose subsets are desired
     * @param size
     *            The size of the subsets of interest
     * @return a set of all subsets of the specified size
     */
    public <T> Set<Set<T>> enumerateSubsets(List<T> v, int size) {
        Set<Set<T>> result = new HashSet<>();
        // 生成 subsets
        backtracking(v, size, 0, new HashSet<>(), result);
        return result;
    }

    private <T> void backtracking(List<T> v, int size, int start, Set<T> current, Set<Set<T>> result) {
        if (current.size() == size) {
            result.add(new HashSet<>(current));  // Add a copy of the current subset
            return;
        }
        for (int i = start; i < v.size(); i++) {
            current.add(v.get(i));
            backtracking(v, size, i + 1, current, result);  // Recur with the next element
            current.remove(v.get(i));  // Backtrack
        }
    }


    /**
     * Compute a logical, reasonably efficient join on the specified tables. See
     * PS4 for hints on how this should be implemented.<br>
     * 计算指定表的逻辑连接，并尽可能高效地执行。有关如何实现的提示，请参见PS4。
     *
     * @param stats
     *            Statistics for each table involved in the join, referenced by
     *            base table names, not alias<br>
     *            各表的统计信息，通过基础表名称引用，不是别名
     * @param filterSelectivities
     *            Selectivities of the filter predicates on each table in the
     *            join, referenced by table alias (if no alias, the base table
     *            name)<br>
     *            每个表上筛选谓词的选择性，通过表别名引用（如果没有别名，则使用基础表名称）
     * @param explain
     *            Indicates whether your code should explain its query plan or
     *            simply execute it<br>
     *            指示代码是否应该解释其查询计划，还是仅仅执行它
     * @return A List<LogicalJoinNode> that stores joins in the left-deep
     *         order in which they should be executed.<br>
     *         返回一个List<LogicalJoinNode>，该列表存储了按左深度顺序排列的连接
     * @throws ParsingException
     *             when stats or filter selectivities is missing a table in the
     *             join, or or when another internal error occurs<br>
     *             当stats或filterSelectivities中缺少某个表，或者发生其他内部错误时抛出
     */
    public List<LogicalJoinNode> orderJoins(
            Map<String, TableStats> stats, // 各表的统计信息
            Map<String, Double> filterSelectivities, // 每个表上筛选谓词的选择性
            boolean explain)
            throws ParsingException {
        // some code goes here
        // 存储最优计划的成本和基数信息
        CostCard bestCostCard = new CostCard();
        // 缓存之前计算的计划，避免重复计算
        PlanCache planCache = new PlanCache();
        // 思路：通过辅助方法获取每个size下最优的连接顺序，不断加入planCache中
        // 遍历连接子集的所有大小
        for (int i = 1; i <= joins.size(); i++) {
            // 获取所有大小为 i 的连接子集
            Set<Set<LogicalJoinNode>> subsets = enumerateSubsets(joins, i); // 好像是个全排列方法
            // 对每个子集进行遍历
            for (Set<LogicalJoinNode> set : subsets) {
                // 记录当前子集的最优成本（初始值为最大值）
                double bestCostSoFar = Double.MAX_VALUE;
                // 重置 bestCostCard，存储当前子集的最优连接计划
                bestCostCard = new CostCard();
                // 遍历当前子集中的每一个连接节点
                for (LogicalJoinNode logicalJoinNode : set) {
                    // 根据当前的子集和连接节点，计算最优连接方式
                    CostCard costCard = computeCostAndCardOfSubplan(
                            stats, filterSelectivities, logicalJoinNode,
                            set, bestCostSoFar, planCache);
                    // 如果返回的costCard为null，表示没有找到合适的连接方案，跳过
                    if (costCard == null) continue;
                    // 更新当前最优成本
                    bestCostSoFar = costCard.cost;
                    // 更新当前最优的成本和计划
                    bestCostCard = costCard;
                }
                // 如果在该子集上找到了有效的最优连接计划
                if (bestCostSoFar != Double.MAX_VALUE) {
                    // 将当前子集的最优连接计划和成本加入planCache缓存中
                    planCache.addPlan(set, bestCostCard.cost, bestCostCard.card, bestCostCard.plan);
                }
            }
        }

        // 如果需要打印查询计划，调用printJoins方法输出
        if (explain){
            printJoins(bestCostCard.plan, planCache, stats, filterSelectivities);
        }

        // 如果最终计算的bestCostCard.plan不为空，则返回最优的连接顺序
        if(bestCostCard.plan != null){
            return bestCostCard.plan;
        }

        // 如果没有找到有效的连接顺序，则返回原始的连接列表
        return joins;
    }

    // ===================== Private Methods =================================

    /**
     * 这是一个辅助方法，计算将joinToRemove连接到joinSet的成本和基数（joinSet应该包含joinToRemove），
     * 给定大小为joinSet的所有子集。size() - 1已经计算并存储在PlanCache pc中。<br>
     * This is a helper method that computes the cost and cardinality of joining
     * joinToRemove to joinSet (joinSet should contain joinToRemove), given that
     * all of the subsets of size joinSet.size() - 1 have already been computed
     * and stored in PlanCache pc.
     *
     * @param stats
     *            table stats for all of the tables, referenced by table names
     *            rather than alias (see {@link #orderJoins})<br>
     *            各表的统计信息
     * @param filterSelectivities
     *            the selectivities of the filters over each of the tables
     *            (where tables are indentified by their alias or name if no
     *            alias is given)<br>
     *            各表的谓词选择性
     * @param joinToRemove
     *            the join to remove from joinSet<br>
     *            遍历当前 Join 子集中的每一个连接节点
     * @param joinSet
     *            the set of joins being considered<br>
     *            当前 Join 子集
     * @param bestCostSoFar
     *            the best way to join joinSet so far (minimum of previous
     *            invocations of computeCostAndCardOfSubplan for this joinSet,
     *            from returned CostCard)<br>
     *            记录当前子集的最优成本
     * @param pc
     *            the PlanCache for this join; should have subplans for all
     *            plans of size joinSet.size()-1<br>
     *            缓存之前的最优计划
     * @return A {@link CostCard} objects desribing the cost, cardinality,
     *         optimal subplan<br>
     *         最优连接方式
     * @throws ParsingException
     *             when stats, filterSelectivities, or pc object is missing
     *             tables involved in join
     */
    @SuppressWarnings("unchecked")
    private CostCard computeCostAndCardOfSubplan(
            Map<String, TableStats> stats, // 各表的统计信息
            Map<String, Double> filterSelectivities, // 各表的谓词选择性（过滤）
            LogicalJoinNode joinToRemove, // 要从 joinSet 中移除的连接
            Set<LogicalJoinNode> joinSet, // 当前 Join 子集
            double bestCostSoFar, // 正在考虑的连接集合
            PlanCache pc // 缓存之前的最优计划
    ) throws ParsingException {

        LogicalJoinNode j = joinToRemove;

        List<LogicalJoinNode> prevBest; // 存储前一个最佳连续子集的计划

        // 检查表是否存在，不存在抛异常
        if (this.p.getTableId(j.t1Alias) == null)
            throw new ParsingException("Unknown table " + j.t1Alias);
        if (this.p.getTableId(j.t2Alias) == null)
            throw new ParsingException("Unknown table " + j.t2Alias);

        //  获取表名
        String table1Name = Database.getCatalog().getTableName(
                this.p.getTableId(j.t1Alias));
        String table2Name = Database.getCatalog().getTableName(
                this.p.getTableId(j.t2Alias));
        String table1Alias = j.t1Alias; // 表 1 的别名
        String table2Alias = j.t2Alias; // 表 2 的别名

        // 从 joinSet 中移除 joinToRemove（移除 join 节点（相当于树上的分叉节点））
        Set<LogicalJoinNode> news = new HashSet<>(joinSet);
        news.remove(j);

        double t1cost, t2cost;  // 表 1 和表 2 的扫描成本
        int t1card, t2card;  // 表 1 和表 2 的基数
        boolean leftPkey, rightPkey;  // 表 1 和表 2 的主键标志

        // 基本情况 -- 两个表都是基本关系
        // 说明当前 joinSet 中只有一个 Join 节点。
        if (news.isEmpty()) { // base case -- both are base relations
            prevBest = new ArrayList<>();
            // 估算表 1 的扫描成本
            t1cost = stats.get(table1Name).estimateScanCost();
            // 估算表 1 的基数
            t1card = stats.get(table1Name).estimateTableCardinality(
                    filterSelectivities.get(j.t1Alias));
            // 判断表 1 是否是主键
            leftPkey = isPkey(j.t1Alias, j.f1PureName);

            // 如果表 2 存在，则估算扫描成本
            t2cost = table2Alias == null ? 0 : stats.get(table2Name)
                    .estimateScanCost();
            // 如果表 2 存在，则估算表 2 的基数
            t2card = table2Alias == null ? 0 : stats.get(table2Name)
                    .estimateTableCardinality(
                            filterSelectivities.get(j.t2Alias));
            // 判断表 2 是否是主键
            rightPkey = table2Alias != null && isPkey(table2Alias,
                    j.f2PureName);
        } else {
            // 两表以上连接
            // 如果 news 不为空，则计算 joinToRemove 与 news 的最优连接
            // news is not empty -- figure best way to join j to news
            prevBest = pc.getOrder(news); // 获取 news 的最佳连接顺序

            // 如果没有缓存的答案（可能是因为子集包含了笛卡尔积），返回 null
            // possible that we have not cached an answer, if subset includes a cross product
            if (prevBest == null) {
                return null;
            }

            double prevBestCost = pc.getCost(news);  // 获取前一个最佳子集的成本
            int bestCard = pc.getCard(news);  // 获取前一个最佳子集的基数

            // 估算右子树的成本
            // estimate cost of right subtree
            // 如果 j.t1 在 prevBest 中
            if (doesJoin(prevBest, table1Alias)) { // j.t1 is in prevBest
                // 左子树的成本为 prevBest 的成本
                t1cost = prevBestCost; // left side just has cost of whatever
                                       // left
                // subtree is
                // 左子树的基数为 prevBest 的基数
                t1card = bestCard;
                // 检查 prevBest 是否有主键
                leftPkey = hasPkey(prevBest);

                // 估算表 2 的扫描成本和基数
                t2cost = j.t2Alias == null ? 0 : stats.get(table2Name)
                        .estimateScanCost();
                t2card = j.t2Alias == null ? 0 : stats.get(table2Name)
                        .estimateTableCardinality(
                                filterSelectivities.get(j.t2Alias));
                // 判断表 2 是否是主键
                rightPkey = j.t2Alias != null && isPkey(j.t2Alias,
                        j.f2PureName);
            }
            // 如果 j.t2 在 prevBest 中
            else if (doesJoin(prevBest, j.t2Alias)) { // j.t2 is in prevbest
                                                        // (both
                // shouldn't be)
                // 左子树的成本为 prevBest 的成本
                t2cost = prevBestCost; // left side just has cost of whatever
                                       // left
                // subtree is
                // 左子树的基数为 prevBest 的基数
                t2card = bestCard;
                // 检查 prevBest 是否有主键
                rightPkey = hasPkey(prevBest);
                // 估算表 1 的扫描成本
                t1cost = stats.get(table1Name).estimateScanCost();
                // 估算表 1 的基数
                t1card = stats.get(table1Name).estimateTableCardinality(
                        filterSelectivities.get(j.t1Alias));
                // 判断表 1 是否是主键
                leftPkey = isPkey(j.t1Alias, j.f1PureName);

            } else {
                // 如果 j.t1 和 j.t2 都不在 prevBest 中，跳过该计划（交叉积）
                // don't consider this plan if one of j.t1 or j.t2
                // isn't a table joined in prevBest (cross product)
                return null;
            }
        }

        // 计算连接成本
        // case where prevbest is left
        double cost1 = estimateJoinCost(j, t1card, t2card, t1cost, t2cost);

        // 交换连接顺序，计算另一种连接顺序的成本
        LogicalJoinNode j2 = j.swapInnerOuter();
        double cost2 = estimateJoinCost(j2, t2card, t1card, t2cost, t1cost);

        // 如果另一种顺序成本更低，交换连接顺序
        if (cost2 < cost1) {
            boolean tmp;
            j = j2;
            cost1 = cost2;
            tmp = rightPkey;
            rightPkey = leftPkey;
            leftPkey = tmp;
        }

        // 如果新计算的成本不优于当前的最佳成本，返回 null
        if (cost1 >= bestCostSoFar)
            return null;

        // 创建 CostCard 对象，保存成本、基数和子计划
        CostCard cc = new CostCard();

        // 估算连接后的基数
        cc.card = estimateJoinCardinality(j, t1card, t2card, leftPkey, rightPkey, stats);
        cc.cost = cost1; // 连接成本
        cc.plan = new ArrayList<>(prevBest); // 前一个最佳子集的连接顺序
        cc.plan.add(j); // 将当前连接添加到连接顺序的末尾 // prevbest is left -- add new join to end
        return cc;  // 返回计算出的成本和基数
    }

    /**
     * Return true if the specified table is in the list of joins, false
     * otherwise
     */
    private boolean doesJoin(List<LogicalJoinNode> joinlist, String table) {
        for (LogicalJoinNode j : joinlist) {
            if (j.t1Alias.equals(table)
                    || (j.t2Alias != null && j.t2Alias.equals(table)))
                return true;
        }
        return false;
    }

    /**
     * Return true if field is a primary key of the specified table, false
     * otherwise
     *
     * @param tableAlias
     *            The alias of the table in the query
     * @param field
     *            The pure name of the field
     */
    private boolean isPkey(String tableAlias, String field) {
        int tid1 = p.getTableId(tableAlias);
        String pkey1 = Database.getCatalog().getPrimaryKey(tid1);

        return pkey1.equals(field);
    }

    /**
     * Return true if a primary key field is joined by one of the joins in
     * joinlist
     */
    private boolean hasPkey(List<LogicalJoinNode> joinlist) {
        for (LogicalJoinNode j : joinlist) {
            if (isPkey(j.t1Alias, j.f1PureName)
                    || (j.t2Alias != null && isPkey(j.t2Alias, j.f2PureName)))
                return true;
        }
        return false;

    }

    /**
     * Helper function to display a Swing window with a tree representation of
     * the specified list of joins. See {@link #orderJoins}, which may want to
     * call this when the analyze flag is true.
     *
     * @param js
     *            the join plan to visualize
     * @param pc
     *            the PlanCache accumulated whild building the optimal plan
     * @param stats
     *            table statistics for base tables
     * @param selectivities
     *            the selectivities of the filters over each of the tables
     *            (where tables are indentified by their alias or name if no
     *            alias is given)
     */
    private void printJoins(List<LogicalJoinNode> js, PlanCache pc,
            Map<String, TableStats> stats,
            Map<String, Double> selectivities) {

        JFrame f = new JFrame("Join Plan for " + p.getQuery());

        // Set the default close operation for the window,
        // or else the program won't exit when clicking close button
        f.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

        f.setVisible(true);

        f.setSize(300, 500);

        Map<String, DefaultMutableTreeNode> m = new HashMap<>();

        // int numTabs = 0;

        // int k;
        DefaultMutableTreeNode root = null, treetop = null;
        HashSet<LogicalJoinNode> pathSoFar = new HashSet<>();
        boolean neither;

        System.out.println(js);
        for (LogicalJoinNode j : js) {
            pathSoFar.add(j);
            System.out.println("PATH SO FAR = " + pathSoFar);

            String table1Name = Database.getCatalog().getTableName(
                    this.p.getTableId(j.t1Alias));
            String table2Name = Database.getCatalog().getTableName(
                    this.p.getTableId(j.t2Alias));

            // Double c = pc.getCost(pathSoFar);
            neither = true;

            root = new DefaultMutableTreeNode("Join " + j + " (Cost ="
                    + pc.getCost(pathSoFar) + ", card = "
                    + pc.getCard(pathSoFar) + ")");
            DefaultMutableTreeNode n = m.get(j.t1Alias);
            if (n == null) { // never seen this table before
                n = new DefaultMutableTreeNode(j.t1Alias
                        + " (Cost = "
                        + stats.get(table1Name).estimateScanCost()
                        + ", card = "
                        + stats.get(table1Name).estimateTableCardinality(
                                selectivities.get(j.t1Alias)) + ")");
                root.add(n);
            } else {
                // make left child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t1Alias, root);

            n = m.get(j.t2Alias);
            if (n == null) { // never seen this table before

                n = new DefaultMutableTreeNode(
                        j.t2Alias == null ? "Subplan"
                                : (j.t2Alias
                                        + " (Cost = "
                                        + stats.get(table2Name)
                                                .estimateScanCost()
                                        + ", card = "
                                        + stats.get(table2Name)
                                                .estimateTableCardinality(
                                                        selectivities
                                                                .get(j.t2Alias)) + ")"));
                root.add(n);
            } else {
                // make right child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t2Alias, root);

            // unless this table doesn't join with other tables,
            // all tables are accessed from root
            if (!neither) {
                for (String key : m.keySet()) {
                    m.put(key, root);
                }
            }

            treetop = root;
        }

        JTree tree = new JTree(treetop);
        JScrollPane treeView = new JScrollPane(tree);

        tree.setShowsRootHandles(true);

        // Set the icon for leaf nodes.
        ImageIcon leafIcon = new ImageIcon("join.jpg");
        DefaultTreeCellRenderer renderer = new DefaultTreeCellRenderer();
        renderer.setOpenIcon(leafIcon);
        renderer.setClosedIcon(leafIcon);

        tree.setCellRenderer(renderer);

        f.setSize(300, 500);

        f.add(treeView);
        for (int i = 0; i < tree.getRowCount(); i++) {
            tree.expandRow(i);
        }

        if (js.size() == 0) {
            f.add(new JLabel("No joins in plan."));
        }

        f.pack();

    }

}
