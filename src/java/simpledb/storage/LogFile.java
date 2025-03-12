
package simpledb.storage;

import simpledb.common.Database;
import simpledb.transaction.TransactionId;
import simpledb.common.Debug;

import java.io.*;
import java.util.*;
import java.lang.reflect.*;

/**
LogFile implements the recovery subsystem of SimpleDb.  This class is
able to write different log records as needed, but it is the
responsibility of the caller to ensure that write ahead logging and
two-phase locking discipline are followed.  <p>

<u> Locking note: </u>
<p>

Many of the methods here are synchronized (to prevent concurrent log
writes from happening); many of the methods in BufferPool are also
synchronized (for similar reasons.)  Problem is that BufferPool writes
log records (on page flushed) and the log file flushes BufferPool
pages (on checkpoints and recovery.)  This can lead to deadlock.  For
that reason, any LogFile operation that needs to access the BufferPool
must not be declared synchronized and must begin with a block like:

<p>
<pre>
    synchronized (Database.getBufferPool()) {
       synchronized (this) {

       ..

       }
    }
</pre>

<p> The format of the log file is as follows:

<ul>

<li> The first long integer of the file represents the offset of the
last written checkpoint, or -1 if there are no checkpoints

<li> All additional data in the log consists of log records.  Log
records are variable length.

<li> Each log record begins with an integer type and a long integer
transaction id.

<li> Each log record ends with a long integer file offset representing
the position in the log file where the record began.

<li> There are five record types: ABORT, COMMIT, UPDATE, BEGIN, and
CHECKPOINT

<li> ABORT, COMMIT, and BEGIN records contain no additional data

<li>UPDATE RECORDS consist of two entries, a before image and an
after image.  These images are serialized Page objects, and can be
accessed with the LogFile.readPageData() and LogFile.writePageData()
methods.  See LogFile.print() for an example.

<li> CHECKPOINT records consist of active transactions at the time
the checkpoint was taken and their first log record on disk.  The format
of the record is an integer count of the number of transactions, as well
as a long integer transaction id and a long integer first record offset
for each active transaction.

</ul>

 <p>翻译：</p>
 <p>
 LogFile 实现了 SimpleDb 的恢复子系统。该类能够根据需要写入不同的日志记录，但调用者有责任确保遵循预写日志和两阶段锁定的规则。
 </p>

 <u> 锁定注意事项： </u>

 <p>
 这里的许多方法都采用了同步机制（以防止并发日志写入发生）；BufferPool 中的许多方法也同样采用了同步机制（出于类似原因）。问题在于，BufferPool 在页面刷新时写入日志记录，而日志文件在检查点和恢复时刷新 BufferPool 的页面，这可能导致死锁。因此，任何需要访问 BufferPool 的 LogFile 操作都不能声明为同步方法，必须以如下代码块开头：
 </p>

 <pre>
    synchronized (Database.getBufferPool()) {
        synchronized (this) {

        ..

        }
    }
 </pre>

 <p>日志文件的格式如下：

 <ul>

 <li> 文件的第一个长整型数值表示最后写入的检查点的偏移量，如果没有检查点则为 -1。

 <li> 日志中所有其他数据均由日志记录组成，且日志记录的长度是可变的。

 <li> 每条日志记录以一个整数类型和一个长整型事务 ID 开始。

 <li> 每条日志记录以一个长整型文件偏移量结束，该偏移量表示该记录在日志文件中开始的位置。

 <li> 有五种记录类型：ABORT、COMMIT、UPDATE、BEGIN 和 CHECKPOINT。

 <li> ABORT、COMMIT 以及 BEGIN 记录不包含额外的数据。

 <li> UPDATE 记录由两个部分组成，一个是修改前的影像，一个是修改后的影像。这些影像是序列化的 Page 对象，可以通过 LogFile.readPageData() 和 LogFile.writePageData() 方法进行访问。参见 LogFile.print() 中的示例。

 <li> CHECKPOINT 记录包含在生成检查点时活跃的事务以及这些事务在磁盘上第一条日志记录的信息。该记录的格式为：一个整数表示活跃事务的数量，后面跟着每个活跃事务的一个长整型事务 ID 和一个长整型第一记录偏移量。

 </ul>
*/
public class LogFile {

    final File logFile;
    private RandomAccessFile raf;
    Boolean recoveryUndecided; // no call to recover() and no append to log

    // 五种日志记录类型
    static final int ABORT_RECORD = 1;      // abort_record
    static final int COMMIT_RECORD = 2;     // commit_record
    static final int UPDATE_RECORD = 3;     // update_record
    static final int BEGIN_RECORD = 4;      // begin_record
    static final int CHECKPOINT_RECORD = 5; // checkpoint_record
    static final long NO_CHECKPOINT_ID = -1;

    final static int INT_SIZE = 4;
    final static int LONG_SIZE = 8;

    long currentOffset = -1;//protected by this
//    int pageSize;
    int totalRecords = 0; // for PatchTest //protected by this

    final Map<Long,Long> tidToFirstLogRecord = new HashMap<>();

    /** Constructor.
        Initialize and back the log file with the specified file.
        We're not sure yet whether the caller is creating a brand new DB,
        in which case we should ignore the log file, or whether the caller
        will eventually want to recover (after populating the Catalog).
        So we make this decision lazily: if someone calls recover(), then
        do it, while if someone starts adding log file entries, then first
        throw out the initial log file contents.

        @param f The log file's name
    */
    public LogFile(File f) throws IOException {
	this.logFile = f;
        raf = new RandomAccessFile(f, "rw");
        recoveryUndecided = true;

        // install shutdown hook to force cleanup on close
        // Runtime.getRuntime().addShutdownHook(new Thread() {
                // public void run() { shutdown(); }
            // });

        //XXX WARNING -- there is nothing that verifies that the specified
        // log file actually corresponds to the current catalog.
        // This could cause problems since we log tableids, which may or
        // may not match tableids in the current catalog.
    }

    /**
     * 我们即将附加一条日志记录。
     * 如果我们之前不确定数据库是否要进行恢复，现在我们可以确定了--它不想。所以要截断日志。<br>
     * we're about to append a log record. if we weren't sure whether the
     * DB wants to do recovery, we're sure now -- it didn't. So truncate
     * the log.
     */
    void preAppend() throws IOException {
        // 总日志记录数加1，表示将要写入一条新的日志记录
        totalRecords++;
        // 如果数据库恢复状态还未确定
        if(recoveryUndecided){
            // 标记恢复状态为确定：数据库不需要进行恢复
            recoveryUndecided = false;
            // 将文件指针移动到日志文件的起始位置（0位置）
            raf.seek(0);
            // 截断日志文件，将其长度设置为0，即清空日志内容
            raf.setLength(0);
            // 在日志文件开头写入一个常量，表示没有检查点（NO_CHECKPOINT_ID）
            raf.writeLong(NO_CHECKPOINT_ID);
            // 将文件指针移动到文件末尾（此时文件长度为刚写入的部分）
            raf.seek(raf.length());
            // 更新 currentOffset 为当前文件指针的位置，即日志最新的偏移量
            currentOffset = raf.getFilePointer();
        }
    }

    public synchronized int getTotalRecords() {
        return totalRecords;
    }

    /**
     * 将指定tid的中止记录写入日志，将日志强制到磁盘，并执行回滚。<br>
     * Write an abort record to the log for the specified tid, force
     * the log to disk, and perform a rollback
     * @param tid The aborting transaction.<br>中止的事务。
     */
    public void logAbort(TransactionId tid) throws IOException {
        // must have buffer pool lock before proceeding, since this
        // calls rollback

        synchronized (Database.getBufferPool()) {

            synchronized(this) {
                preAppend();
                //Debug.log("ABORT");
                //should we verify that this is a live transaction?

                // must do this here, since rollback only works for
                // live transactions (needs tidToFirstLogRecord)
                // 在写入中止日志之前，先执行回滚操作。因为 rollback 仅适用于活跃事务，
                // 且其需要依赖 tidToFirstLogRecord 中保存的事务起始日志位置
                rollback(tid);

                // 写入中止记录标识（ABORT_RECORD）到日志文件
                raf.writeInt(ABORT_RECORD);
                // 写入事务 ID 到日志文件
                raf.writeLong(tid.getId());
                // 写入当前日志文件的偏移量，记录该日志记录的起始位置
                raf.writeLong(currentOffset);
                // 更新 currentOffset 为当前文件指针的位置（即追加记录后的新位置）
                currentOffset = raf.getFilePointer();
                // 强制将日志缓冲区中的数据刷新到磁盘，确保日志持久化
                force();
                // 从事务起始记录映射中移除该事务，以标记该事务已结束
                tidToFirstLogRecord.remove(tid.getId());
            }
        }
    }

    /**
     * 将提交记录写入磁盘，强制刷新日志到磁盘中。<br>
     * Write a commit record to disk for the specified tid,
     * and force the log to disk.
     *
     * @param tid The committing transaction.
     */
    public synchronized void logCommit(TransactionId tid) throws IOException {
        preAppend();
        Debug.log("COMMIT " + tid.getId());
        //should we verify that this is a live transaction?

        raf.writeInt(COMMIT_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();
        force();
        tidToFirstLogRecord.remove(tid.getId());
    }

    /**
     * 将指定事务和页面更新记录写入日志（包括更新前和更新后的页面镜像）。<br>
     * Write an UPDATE record to disk for the specified tid and page
     * (with provided before and after images.)
     *
     * @param tid    执行写操作的事务
     * @param before 写操作前的页面数据（页面的 before image）
     * @param after  写操作后的页面数据（页面的 after image）
     *
     * @see Page#getBeforeImage
     */
    public  synchronized void logWrite(TransactionId tid, Page before,
                                       Page after)
        throws IOException  {
        Debug.log("WRITE, offset = " + raf.getFilePointer());
        preAppend();
        /* update record conists of

           record type
           transaction id
           before page data (see writePageData)
           after page data
           start offset
        */
        /*
            写入更新记录到日志文件的内容包括：

            - 记录类型（UPDATE_RECORD）
            - 事务ID
            - 更新前的页面数据（通过 writePageData 方法写入）
            - 更新后的页面数据（通过 writePageData 方法写入）
            - 当前日志记录的起始偏移量
        */
        // 写入更新记录的标识（UPDATE_RECORD）到日志文件
        raf.writeInt(UPDATE_RECORD);
        // 写入事务 ID 到日志文件
        raf.writeLong(tid.getId());

        // 写入更新前页面的数据到日志文件
        writePageData(raf, before);
        // 写入更新后页面的数据到日志文件
        writePageData(raf, after);
        // 写入当前日志文件偏移量，作为本次写入日志记录的起始位置
        raf.writeLong(currentOffset);
        // 更新 currentOffset 为当前文件指针的位置
        currentOffset = raf.getFilePointer();

        Debug.log("WRITE OFFSET = " + currentOffset);
    }

    void writePageData(RandomAccessFile raf, Page p) throws IOException{
        PageId pid = p.getId();
        int[] pageInfo = pid.serialize();

        //page data is:
        // page class name
        // id class name
        // id class bytes
        // id class data
        // page class bytes
        // page class data

        String pageClassName = p.getClass().getName();
        String idClassName = pid.getClass().getName();

        raf.writeUTF(pageClassName);
        raf.writeUTF(idClassName);

        raf.writeInt(pageInfo.length);
        for (int j : pageInfo) {
            raf.writeInt(j);
        }
        byte[] pageData = p.getPageData();
        raf.writeInt(pageData.length);
        raf.write(pageData);
        //        Debug.log ("WROTE PAGE DATA, CLASS = " + pageClassName + ", table = " +  pid.getTableId() + ", page = " + pid.pageno());
    }

    Page readPageData(RandomAccessFile raf) throws IOException {
        PageId pid;
        Page newPage = null;

        String pageClassName = raf.readUTF();
        String idClassName = raf.readUTF();

        try {
            Class<?> idClass = Class.forName(idClassName);
            Class<?> pageClass = Class.forName(pageClassName);

            Constructor<?>[] idConsts = idClass.getDeclaredConstructors();
            int numIdArgs = raf.readInt();
            Object[] idArgs = new Object[numIdArgs];
            for (int i = 0; i<numIdArgs;i++) {
                idArgs[i] = raf.readInt();
            }
            pid = (PageId)idConsts[0].newInstance(idArgs);

            Constructor<?>[] pageConsts = pageClass.getDeclaredConstructors();
            int pageSize = raf.readInt();

            byte[] pageData = new byte[pageSize];
            raf.read(pageData); //read before image

            Object[] pageArgs = new Object[2];
            pageArgs[0] = pid;
            pageArgs[1] = pageData;

            newPage = (Page)pageConsts[0].newInstance(pageArgs);

            //            Debug.log("READ PAGE OF TYPE " + pageClassName + ", table = " + newPage.getId().getTableId() + ", page = " + newPage.getId().pageno());
        } catch (ClassNotFoundException | InvocationTargetException | IllegalAccessException | InstantiationException e){
            e.printStackTrace();
            throw new IOException();
        }
        return newPage;

    }

    /** Write a BEGIN record for the specified transaction
        @param tid The transaction that is beginning

    */
    public synchronized  void logXactionBegin(TransactionId tid)
        throws IOException {
        Debug.log("BEGIN");
        // 如果当前事务已经开始，则不能重复记录
        if(tidToFirstLogRecord.get(tid.getId()) != null){
            System.err.print("logXactionBegin: already began this tid\n");
            throw new IOException("double logXactionBegin()");
        }

        // 调用 preAppend() 方法进行预处理，确保日志文件处于追加数据的准备状态
        preAppend();

        // 向日志文件中写入一个整数常量 BEGIN_RECORD，
        // 用以标识接下来的日志记录属于事务开始记录
        raf.writeInt(BEGIN_RECORD);

        // 将事务的唯一标识 ID 写入到日志文件中（以 long 类型写入）
        raf.writeLong(tid.getId());

        // 将当前日志文件的偏移量写入到日志中，
        // 用于记录该事务开始记录的起始位置，便于后续恢复或管理
        raf.writeLong(currentOffset);

        // 在映射 tidToFirstLogRecord 中记录事务ID和对应的日志起始偏移量，
        // 以便以后可以通过事务ID查找它的日志记录位置
        tidToFirstLogRecord.put(tid.getId(), currentOffset);

        // 更新 currentOffset，将其设置为当前文件指针的位置，
        // 这样 currentOffset 就指向了日志文件的最新位置（写入结束后的位置）
        currentOffset = raf.getFilePointer();

        // 输出调试日志，显示写入完毕后新的日志文件偏移量
        Debug.log("BEGIN OFFSET = " + currentOffset);
    }

    /** Checkpoint the log and write a checkpoint record. */
    public void logCheckpoint() throws IOException {
        //make sure we have buffer pool lock before proceeding
        // 确保在继续操作之前获取缓冲池锁
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                //Debug.log("CHECKPOINT, offset = " + raf.getFilePointer());
                preAppend(); // 为日志追加操作做准备
                long startCpOffset, endCpOffset;
                Set<Long> keys = tidToFirstLogRecord.keySet(); // 获取所有事务的ID集合
                Iterator<Long> els = keys.iterator();
                force(); // 强制将缓存写入磁盘
                Database.getBufferPool().flushAllPages(); // 刷新所有页面到磁盘
                startCpOffset = raf.getFilePointer(); // 获取当前文件指针位置
                raf.writeInt(CHECKPOINT_RECORD); // 写入检查点记录类型标识
                raf.writeLong(-1); // 写入事务ID，-1表示没有事务ID，但为方便保留空间 //no tid , but leave space for convenience

                //write list of outstanding transactions
                // 写入所有待处理事务的列表
                raf.writeInt(keys.size()); // 写入待处理事务的数量
                while (els.hasNext()) {
                    Long key = els.next();
                    Debug.log("WRITING CHECKPOINT TRANSACTION ID: " + key); // 打印事务ID
                    raf.writeLong(key); // 写入事务ID
                    //Debug.log("WRITING CHECKPOINT TRANSACTION OFFSET: " + tidToFirstLogRecord.get(key));
                    raf.writeLong(tidToFirstLogRecord.get(key)); // 写入事务的日志记录偏移量
                }

                //once the CP is written, make sure the CP location at the
                // beginning of the log file is updated
                // 一旦检查点记录写入，确保更新日志文件开头的检查点位置
                endCpOffset = raf.getFilePointer(); // 获取当前文件指针位置
                raf.seek(0); // 定位到文件开头
                raf.writeLong(startCpOffset); // 写入检查点记录的起始偏移量
                raf.seek(endCpOffset); // 定位到检查点记录的结束位置
                raf.writeLong(currentOffset); // 写入当前日志记录偏移量
                currentOffset = raf.getFilePointer(); // 更新当前偏移量
                //Debug.log("CP OFFSET = " + currentOffset);
            }
        }

        logTruncate(); // 执行日志截断
    }

    /** Truncate any unneeded portion of the log to reduce its space
        consumption */
    public synchronized void logTruncate() throws IOException {
        preAppend(); // 准备日志追加操作
        raf.seek(0); // 定位到日志文件开头
        long cpLoc = raf.readLong(); // 读取检查点位置

        long minLogRecord = cpLoc; // 初始化最小日志记录位置为检查点位置

        if (cpLoc != -1L) { // 如果检查点位置有效
            raf.seek(cpLoc); // 定位到检查点位置
            int cpType = raf.readInt(); // 读取检查点类型
            @SuppressWarnings("unused")
            long cpTid = raf.readLong(); // 读取事务ID（无用）

            // 如果读取到的不是检查点记录类型，则抛出异常
            if (cpType != CHECKPOINT_RECORD) {
                throw new RuntimeException("Checkpoint pointer does not point to checkpoint record");
            }

            int numOutstanding = raf.readInt(); // 读取待处理事务的数量

            // 遍历所有待处理事务并更新最小日志记录位置
            for (int i = 0; i < numOutstanding; i++) {
                @SuppressWarnings("unused")
                long tid = raf.readLong(); // 读取事务ID
                long firstLogRecord = raf.readLong(); // 读取该事务的第一个日志记录位置
                if (firstLogRecord < minLogRecord) {
                    minLogRecord = firstLogRecord; // 更新最小日志记录位置
                }
            }
        }

        // we can truncate everything before minLogRecord
        // 可以截断掉所有在最小日志记录位置之前的部分
        File newFile = new File("logtmp" + System.currentTimeMillis()); // 创建临时文件
        RandomAccessFile logNew = new RandomAccessFile(newFile, "rw"); // 创建新的随机访问文件
        logNew.seek(0); // 定位到新文件开头
        logNew.writeLong((cpLoc - minLogRecord) + LONG_SIZE); // 写入截断后的长度信息

        raf.seek(minLogRecord); // 定位到最小日志记录位置

        //have to rewrite log records since offsets are different after truncation
        // 由于截断后偏移量发生变化，需要重新写入日志记录
        while (true) {
            try {
                int type = raf.readInt(); // 读取记录类型
                long record_tid = raf.readLong(); // 读取事务ID
                long newStart = logNew.getFilePointer(); // 获取新文件的起始偏移量

                Debug.log("NEW START = " + newStart);

                logNew.writeInt(type); // 写入记录类型
                logNew.writeLong(record_tid); // 写入事务ID

                // 根据记录类型执行不同的写入操作
                switch (type) {
                    case UPDATE_RECORD: // 更新记录类型
                        Page before = readPageData(raf); // 读取更新前的页面数据
                        Page after = readPageData(raf); // 读取更新后的页面数据

                        writePageData(logNew, before); // 写入更新前的页面数据
                        writePageData(logNew, after); // 写入更新后的页面数据
                        break;
                    case CHECKPOINT_RECORD: // 检查点记录类型
                        int numXactions = raf.readInt(); // 读取待处理事务数量
                        logNew.writeInt(numXactions); // 写入待处理事务数量
                        while (numXactions-- > 0) {
                            long xid = raf.readLong(); // 读取事务ID
                            long xoffset = raf.readLong(); // 读取事务的日志记录偏移量
                            logNew.writeLong(xid); // 写入事务ID
                            logNew.writeLong((xoffset - minLogRecord) + LONG_SIZE); // 写入更新后的偏移量
                        }
                        break;
                    case BEGIN_RECORD: // 事务开始记录类型
                        tidToFirstLogRecord.put(record_tid, newStart); // 记录事务ID与其开始日志的偏移量
                        break;
                }

                //all xactions finish with a pointer
                // 所有事务最后都会写入一个指向下一个记录的指针
                logNew.writeLong(newStart); // 写入新的记录起始偏移量
                raf.readLong(); // 读取并忽略原日志中的结束指针

            } catch (EOFException e) {
                break; // 到达文件末尾，结束循环
            }
        }

        Debug.log("TRUNCATING LOG;  WAS " + raf.length() + " BYTES ; NEW START : " + minLogRecord + " NEW LENGTH: " + (raf.length() - minLogRecord));

        raf.close(); // 关闭旧日志文件
        logFile.delete(); // 删除旧日志文件
        newFile.renameTo(logFile); // 将新文件重命名为日志文件
        raf = new RandomAccessFile(logFile, "rw"); // 打开新的日志文件
        raf.seek(raf.length()); // 定位到新文件的末尾
        newFile.delete(); // 删除临时文件

        currentOffset = raf.getFilePointer(); // 更新当前偏移量
        //print();
    }

    /**
     * 回滚指定的事务，将其更新的任何页面的状态设置为其更新前的状态。
     * 为了保持事务语义，不应该在已经提交的事务上调用该方法（尽管此方法可能不会强制执行）。<br>
     * Rollback the specified transaction, setting the state of any
     * of pages it updated to their pre-updated state.  To preserve
     * transaction semantics, this should not be called on
     * transactions that have already committed (though this may not
     * be enforced by this method.)
     *
     * @param tid The transaction to rollback
     */
    public void rollback(TransactionId tid)
        throws NoSuchElementException, IOException {
        synchronized (Database.getBufferPool()) {
            synchronized(this) {
                // 在向日志追加记录前，调用 preAppend()，如果第一次写则会截断旧日志
                preAppend();
                // some code goes here

                // 从 tidToFirstLogRecord 中获取该事务在日志文件中第一条日志记录的偏移量
                Long logRecord = tidToFirstLogRecord.get(tid.getId());

                // 将文件指针移动到该偏移量处，准备从这里开始读取对应事务的日志
                raf.seek(logRecord);

                // 开始循环读取日志，遇到与当前事务相关的 UPDATE 记录就进行回滚
                while (true) {
                    try {
                        // 读取日志记录类型
                        int cpType = raf.readInt();
                        // 读取该日志对应的事务ID
                        long cpTid = raf.readLong();

                        // 如果日志记录类型是 UPDATE_RECORD，则往下读取 before/after page
                        if (cpType == UPDATE_RECORD) {
                            // 读取 update_record 中的 before-image
                            Page before = readPageData(raf);

                            // 如果该 update 记录属于当前要回滚的事务
                            if (tid.getId() == cpTid) {
                                // 将 before-image 写回磁盘，覆盖掉此前的修改
                                Database.getCatalog().getDatabaseFile(before.getId().getTableId()).writePage(before);
                                // 同时把对应 page 从 BufferPool 中剔除，确保内存中不再持有已被覆盖的数据
                                Database.getBufferPool().discardPage(before.getId());
                            }
                        }

                        // 每条日志记录末尾都存有一个 long 型数值，表示该记录开始的偏移量，在此读掉以移动文件指针
                        raf.readLong();  // 这里读取完后，才算完整跳到下一条日志记录
                    } catch (EOFException e) {
                        // 如果读到文件末尾，直接 break 退出循环
                        break;
                    }
                }
            }
        }
    }

    /**
     * 关闭日志系统，写出任何必要的状态，以便快速启动（无需大量恢复）。<br>
     * Shutdown the logging system, writing out whatever state
     * is necessary so that start up can happen quickly (without
     * extensive recovery.)
    */
    public synchronized void shutdown() {
        try {
            logCheckpoint();  //simple way to shutdown is to write a checkpoint record
            raf.close();
        } catch (IOException e) {
            System.out.println("ERROR SHUTTING DOWN -- IGNORING.");
            e.printStackTrace();
        }
    }

    /**
     * 通过确保已提交事务的更新已安装，未提交事务的更新未安装来恢复数据库系统。<br>
     * Recover the database system by ensuring that the updates of
     * committed transactions are installed and that the
     * updates of uncommitted transactions are not installed.
     */
    public void recover() throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                // 一旦调用 recover，就表明我们要基于旧日志进行恢复，不再清空重写
                recoveryUndecided = false;
                // some code goes here

                // 用来存储“已经提交（COMMIT）”的事务ID
                Set<Long> committedId = new HashSet<>();
                // 存储每个事务对应的所有 beforePage（回滚用）
                Map<Long, List<Page>> beforePages = new HashMap<>();
                // 存储每个事务对应的所有 afterPage（提交时的最终状态）
                Map<Long, List<Page>> afterPages = new HashMap<>();

                // 从头开始顺序读取日志文件
                while (true) {
                    try {
                        // 先读日志类型
                        int type = raf.readInt();
                        // 读当前记录所属的事务ID
                        long txid = raf.readLong();

                        // 根据记录类型做不同处理
                        switch (type) {
                            // UPDATE_RECORD：读出 beforePage/afterPage 分别存入两个 map
                            case UPDATE_RECORD:
                                // 先读出前镜像
                                Page beforeImage = readPageData(raf);
                                // 再读出后镜像
                                Page afterImage = readPageData(raf);

                                // 把 beforeImage 加入 beforePages 的列表
                                List<Page> l1 = beforePages.getOrDefault(txid, new ArrayList<>());
                                l1.add(beforeImage);
                                beforePages.put(txid, l1);

                                // 把 afterImage 加入 afterPages 的列表
                                List<Page> l2 = afterPages.getOrDefault(txid, new ArrayList<>());
                                l2.add(afterImage);
                                afterPages.put(txid, l2);
                                break;

                            // COMMIT_RECORD：如果事务提交，则将事务ID放入 committedId
                            case COMMIT_RECORD:
                                committedId.add(txid);
                                break;

                            // CHECKPOINT_RECORD、BEGIN_RECORD 或 ABORT_RECORD 或其他情况，这里只简单忽略
                            default:
                                break;
                        }

                        // 跳过本条记录末尾的“记录起始偏移量”(long)
                        raf.readLong();
                    } catch (EOFException e) {
                        // 如果文件读到末尾，就结束循环
                        break;
                    }
                }

                // 做恢复：对于“未提交”的事务，把它们的 beforeImage 写回磁盘
                for (long txid : beforePages.keySet()) {
                    // 如果这个事务没出现在 committedId 中，说明它没提交，要回滚
                    if (!committedId.contains(txid)) {
                        // 把它所有的 beforeImages 逐一写回磁盘
                        List<Page> pages = beforePages.get(txid);
                        for (Page p : pages) {
                            Database.getCatalog().getDatabaseFile(p.getId().getTableId()).writePage(p);
                        }
                    }
                }

                // 对于已提交的事务，把 afterImage 写回，完成它们的更新
                for (long txid : committedId) {
                    // 如果存在 afterPages 记录（有 UPDATE 过）
                    if (afterPages.containsKey(txid)) {
                        List<Page> pages = afterPages.get(txid);
                        // 将该事务修改过的页面最终状态写回磁盘
                        for (Page page : pages) {
                            Database.getCatalog().getDatabaseFile(page.getId().getTableId()).writePage(page);
                        }
                    }
                }
            }
        }
    }

    /** Print out a human readable represenation of the log */
    public void print() throws IOException {
        long curOffset = raf.getFilePointer();

        raf.seek(0);

        System.out.println("0: checkpoint record at offset " + raf.readLong());

        while (true) {
            try {
                int cpType = raf.readInt();
                long cpTid = raf.readLong();

                System.out.println((raf.getFilePointer() - (INT_SIZE + LONG_SIZE)) + ": RECORD TYPE " + cpType);
                System.out.println((raf.getFilePointer() - LONG_SIZE) + ": TID " + cpTid);

                switch (cpType) {
                case BEGIN_RECORD:
                    System.out.println(" (BEGIN)");
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                    break;
                case ABORT_RECORD:
                    System.out.println(" (ABORT)");
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                    break;
                case COMMIT_RECORD:
                    System.out.println(" (COMMIT)");
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                    break;

                case CHECKPOINT_RECORD:
                    System.out.println(" (CHECKPOINT)");
                    int numTransactions = raf.readInt();
                    System.out.println((raf.getFilePointer() - INT_SIZE) + ": NUMBER OF OUTSTANDING RECORDS: " + numTransactions);

                    while (numTransactions-- > 0) {
                        long tid = raf.readLong();
                        long firstRecord = raf.readLong();
                        System.out.println((raf.getFilePointer() - (LONG_SIZE + LONG_SIZE)) + ": TID: " + tid);
                        System.out.println((raf.getFilePointer() - LONG_SIZE) + ": FIRST LOG RECORD: " + firstRecord);
                    }
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

                    break;
                case UPDATE_RECORD:
                    System.out.println(" (UPDATE)");

                    long start = raf.getFilePointer();
                    Page before = readPageData(raf);

                    long middle = raf.getFilePointer();
                    Page after = readPageData(raf);

                    System.out.println(start + ": before image table id " + before.getId().getTableId());
                    System.out.println((start + INT_SIZE) + ": before image page number " + before.getId().getPageNumber());
                    System.out.println((start + INT_SIZE) + " TO " + (middle - INT_SIZE) + ": page data");

                    System.out.println(middle + ": after image table id " + after.getId().getTableId());
                    System.out.println((middle + INT_SIZE) + ": after image page number " + after.getId().getPageNumber());
                    System.out.println((middle + INT_SIZE) + " TO " + (raf.getFilePointer()) + ": page data");

                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

                    break;
                }

            } catch (EOFException e) {
                //e.printStackTrace();
                break;
            }
        }

        // Return the file pointer to its original position
        raf.seek(curOffset);
    }

    public  synchronized void force() throws IOException {
        raf.getChannel().force(true);
    }

}
