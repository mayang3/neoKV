package com.neoKV.neoKVServer.merge_compaction.lock;

import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author neo82
 */
public class CompactionReadWriteLock {
    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public static ReentrantReadWriteLock.ReadLock readLock() {
        return lock.readLock();
    }

    public static  ReentrantReadWriteLock.WriteLock writeLock() {
        return lock.writeLock();
    }
}
