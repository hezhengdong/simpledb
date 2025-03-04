package simpledb.transaction;

import java.util.Objects;

public class Lock {

    // 锁定的事务
    private TransactionId tid;
    // 锁的类型
    private int lockType;

    public Lock(TransactionId tid, int lockType) {
        this.tid = tid;
        this.lockType = lockType;
    }

    public TransactionId getTid() {
        return tid;
    }

    public void setTid(TransactionId tid) {
        this.tid = tid;
    }

    public int getLockType() {
        return lockType;
    }

    public void setLockType(int lockType) {
        this.lockType = lockType;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Lock)) return false;
        Lock lock = (Lock) o;
        return lockType == lock.lockType && Objects.equals(tid, lock.tid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tid, lockType);
    }
}
