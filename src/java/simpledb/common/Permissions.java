package simpledb.common;

/**
 * 表示对关系/文件的请求权限的枚举类。<br>
 * Class representing requested permissions to a relation/file.
 * Private constructor with two static objects READ_ONLY and READ_WRITE that
 * represent the two levels of permission.
 */
public enum Permissions {
    // 两种权限级别
    READ_ONLY, // 只读。共享锁，即读锁，当一个事务为页加读锁后，其他事务可以为页面加读锁，但是不能加写锁。
    READ_WRITE // 读写。独占锁，即写锁，当一个事务为页加写锁后，其他事务无法对该页面加任何类型的锁，无论读锁、写锁。
}
