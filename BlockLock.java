package com.zheng.store;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * @author zheng
 */
public class BlockLock {
    private AtomicBoolean[] locks = new AtomicBoolean[UnLockQueue.MAX_BLOCK];

    public BlockLock() {
        for (int i = 0;i<UnLockQueue.MAX_BLOCK;i++){
            locks[i] = new AtomicBoolean(false);
        }
    }

    /**
     * 就是index % 128
     * @param index
     * @return
     */
    public static int getMod(int index){
        return index&((1<<8)-1);
    }
    public boolean lock(int index){
//        System.out.println("get lock at "+index);

        while (true){
            if (locks[index%UnLockQueue.MAX_BLOCK].compareAndSet(false,true)){
                return true;
            }
            LockSupport.parkNanos(UnLockQueue.THREAD_PARK_NANOS);
        }

    }

    public void unLock(int index){
        locks[index%UnLockQueue.MAX_BLOCK].set(false);
    }
}
