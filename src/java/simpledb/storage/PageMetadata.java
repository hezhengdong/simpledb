package simpledb.storage;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

public class PageMetadata {
    private long[] accessHistory;
    private long lastAccessTime; // lastAccessTime 与 accessHistory[k - 1] 等价，为什么还要维护它？- 留个坑，为了后续支持拓展“相关引用期”。
    private int k;

    public PageMetadata(int k, long currentTime) {
        this.k = k;
        this.accessHistory = new long[k];
        Arrays.fill(accessHistory, 0);
        this.lastAccessTime = currentTime;
        this.accessHistory[k - 1] = currentTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public long getBackwardKDistance() {
        return accessHistory[0] == 0 ? Long.MAX_VALUE : // 未满K次访问时视为无限大
                System.currentTimeMillis() - accessHistory[0];
    }

    public void updateAccess(long currentTime) {
        // 左移数组，移除最旧时间
        System.arraycopy(accessHistory, 1, accessHistory, 0, k - 1);
        // 更新时间
        this.lastAccessTime = currentTime;
        this.accessHistory[k - 1] = currentTime;
    }
}
