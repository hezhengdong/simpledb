package simpledb;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.*;
import simpledb.storage.HeapFile;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionId;

import java.io.*;

public class jointest {

    public static void main(String[] argv) {
        // construct a 3-column table schema
        // 构建一个包含3列的表的元数据（字段类型和字段名）
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE}; // 字段类型
        String names[] = new String[]{"field0", "field1", "field2"}; // 字段名

        // 创建 TupleDesc 对象来描述表结构
        TupleDesc td = new TupleDesc(types, names);

        // create the tables, associate them with the data files
        // and tell the catalog about the schema  the tables.
        // 创建堆文件 table1，将文件与元数据关联，并告诉目录该表的元数据
        HeapFile table1 = new HeapFile(new File("some_data_file1.dat"), td);
        Database.getCatalog().addTable(table1, "t1");  // 将表添加到数据库的目录中，表的别名为 t1

        // 创建堆文件 table2，将文件与元数据关联，并告诉目录该表的元数据
        HeapFile table2 = new HeapFile(new File("some_data_file2.dat"), td);
        Database.getCatalog().addTable(table2, "t2");

        // construct the query: we use two SeqScans, which spoonfeed
        // tuples via iterators into join
        // 创建一个事务ID用于管理数据库操作
        TransactionId tid = new TransactionId();

        // 创建两个顺序扫描（SeqScan）操作符，用于遍历两个表的数据
        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // create a filter for the where condition
        // 创建一个过滤器，应用于 t1 表的数据，筛选出 field0 > 1 的记录
        Filter sf1 = new Filter(
                new Predicate(0,  // 选择 t1 表中的 field0 字段
                        Predicate.Op.GREATER_THAN,  // 选择大于（>）操作符
                        new IntField(1)),  // 与常数 1 进行比较
                ss1);  // 过滤器应用在 t1 表的 SeqScan 上

        // 创建连接谓词，定义连接条件：t1.field1 = t2.field1
        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);

        // 创建连接操作符，使用 JoinPredicate，将过滤后的 t1 数据与 t2 数据连接
        Join j = new Join(p, sf1, ss2);

        // and run it
        try {
            j.open(); // 打开连接操作符，开始执行查询
            while (j.hasNext()) { // 遍历查询结果
                Tuple tup = j.next(); // 获取下一个连接的元组
                System.out.println(tup); // 打印元组
            }
            j.close(); // 查询完成，关闭连接操作符
            Database.getBufferPool().transactionComplete(tid); // 完成事务，提交操作

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}