package simpledb.common;

import simpledb.storage.BufferPool;
import simpledb.storage.LogFile;

import java.io.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Database is a class that initializes several static variables used by the
 * database system (the catalog, the buffer pool, and the log files, in
 * particular.)
 * <p>
 * Provides a set of methods that can be used to access these variables from
 * anywhere.
 * <p>
 * 该类的主要功能是初始化数据库系统中用到的静态变量（目录、缓冲池、日志文件），并提供全局访问方法。
 * 
 * @Threadsafe 表明该类线程安全
 */
public class Database {
    // 单例模式、线程安全
    private static final AtomicReference<Database> _instance = new AtomicReference<>(new Database());
    // 目录
    private final Catalog _catalog;
    // 缓冲池
    private final BufferPool _bufferpool;

    private final static String LOGFILENAME = "log";
    // 日志文件
    private final LogFile _logfile;

    private Database() {
        _catalog = new Catalog();
        _bufferpool = new BufferPool(BufferPool.DEFAULT_PAGES);
        LogFile tmp = null;
        try {
            tmp = new LogFile(new File(LOGFILENAME));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        _logfile = tmp;
        // startControllerThread();
    }

    /** Return the log file of the static Database instance */
    public static LogFile getLogFile() {
        return _instance.get()._logfile;
    }

    /** Return the buffer pool of the static Database instance */
    public static BufferPool getBufferPool() {
        return _instance.get()._bufferpool;
    }

    /** Return the catalog of the static Database instance */
    public static Catalog getCatalog() {
        return _instance.get()._catalog;
    }

    // 下面两个方法仅用于单元测试，一个是通过反射重置缓冲池，一个是重置整个数据库实例
    /**
     * Method used for testing -- create a new instance of the buffer pool and
     * return it
     */
    public static BufferPool resetBufferPool(int pages) {
        java.lang.reflect.Field bufferPoolF=null;
        try {
            bufferPoolF = Database.class.getDeclaredField("_bufferpool");
            bufferPoolF.setAccessible(true);
            bufferPoolF.set(_instance.get(), new BufferPool(pages));
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }
//        _instance._bufferpool = new BufferPool(pages);
        return _instance.get()._bufferpool;
    }

    // reset the database, used for unit tests only.
    public static void reset() {
        _instance.set(new Database());
    }

}
