package com.zheng.store;

import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * @author zheng
 */
public class SimpleRecycle {

    private static final int CAPACITY = 1024 *  1024;
    private SoftReference<LRCAlgorition<Integer,byte[]>> lengthMap = new SoftReference<>(new LRCAlgorition<>(CAPACITY));
    private static AtomicBoolean lock = new AtomicBoolean(false);
    public void lock(){
        while (true){
            if (lock.compareAndSet(false,true)){
                return;
            }
            LockSupport.parkNanos(30);

        }
    }
    public static SimpleRecycle getInstance(){
        return Holder.INSTANCE;
    }
    private static class Holder{
        private static SimpleRecycle INSTANCE = new SimpleRecycle();
    }
    public void unLock(){
        lock.set(false);
    }
    public byte[] getNotNullBytes(int length){

        byte[] bytes = getByte(length);
        if (bytes != null){
            return bytes;
        }else {
            return getNotNullBytes(length);
        }
    }
    public byte[] getByte(int length){
        if (length > CAPACITY){
            throw new IndexOutOfBoundsException("large than capacity "+length);
        }
        try {
            LRCAlgorition<Integer, byte[]> map = lengthMap.get();
            if (map == null){
                lengthMap = new SoftReference<>(new LRCAlgorition<>(CAPACITY));
            }
            byte[] bytes = map.getNode(length);
            if (bytes == null){
                bytes = new byte[length];
                map.put(length,bytes,bytes.length);
            }
            return bytes;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }

    }
    public byte[] getWithLock(int length){
        if (length <= 0){
            throw new RuntimeException("length not wright -----"+length);
        }
        lock();
        try {
            return getNotNullBytes(length);
        }finally {
            unLock();
        }
//        return getByte(length);
    }
}
