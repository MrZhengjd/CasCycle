package com.zheng.store;

import java.io.Serializable;

import static com.zheng.store.QueueBlock.BLOCK_SIZE;

/**
 * @author zheng
 * 返回操作区间的writerIndex 和readerIndex
 * 先获取位置，然后操作
 * 这样一来cas的时间就会短一些，不必进行操作
 */
public class BlockInfo implements Cloneable, Serializable {
    //当前写的位置
    private int currentWriterIndex;
    //写后数据的位置
    private int afterWriteIndex;

    private int startwritePosition;
    //当前读的位置
    private int currentReaderIndex;
    //读后数据的位置
    private int afterReaderIndex;
    private int startReadPositon;

    private int length;

    private boolean unReach;
    private static class Holder{
        private static BlockInfo INSTANCE = new BlockInfo();
    }
    public static BlockInfo getInstance(){
        return Holder.INSTANCE;
//        try {
//            return (BlockInfo) Holder.INSTANCE.clone();
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        return new BlockInfo();
    }
    public void initData(){
        unReach = false;
        length = 0;
        startReadPositon = 0;
        afterReaderIndex = 0;
        startwritePosition = 0;
        currentReaderIndex = 0;
        currentWriterIndex = 0;
        afterWriteIndex = 0;
    }
    @Override
    public String toString() {
        return "current reader index "+currentReaderIndex +" length "+length +" after reader index "+afterReaderIndex + " after writer index "+afterWriteIndex;
    }

    public boolean isUnReach() {
        return unReach;
    }

    public void setUnReach(boolean unReach) {
        this.unReach = unReach;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getStartwritePosition() {
        return startwritePosition;
    }

    public void setStartwritePosition(int startwritePosition) {
        this.startwritePosition = startwritePosition;
    }

    public int getStartReadPositon() {
        return startReadPositon;
    }

    public void setStartReadPositon(int startReadPositon) {
        this.startReadPositon = startReadPositon;
    }

    //页的index
    private int pageIndex;

    public BlockInfo() {
    }
    public void setBaseInfo(int currentWriterIndex, int currentReaderIndex, int pageIndex){
        this.currentWriterIndex = currentWriterIndex;
        this.currentReaderIndex = currentReaderIndex;
        this.pageIndex = pageIndex;
    }

    public BlockInfo(int currentWriterIndex, int currentReaderIndex, int pageIndex) {
        this.currentWriterIndex = currentWriterIndex;
        this.currentReaderIndex = currentReaderIndex;
        this.pageIndex = pageIndex;
    }

    public int getCurrentReaderIndex() {
        return currentReaderIndex;
    }

    public void setCurrentReaderIndex(int currentReaderIndex) {
        this.currentReaderIndex = currentReaderIndex;
    }

    public int getAfterReaderIndex() {
        return afterReaderIndex;
    }

    public void setAfterReaderIndex(int afterReaderIndex) {
        this.afterReaderIndex = afterReaderIndex;
    }

    public int getPageIndex() {
        return pageIndex;
    }

    public void setPageIndex(int pageIndex) {
        this.pageIndex = pageIndex;
    }

    public int getCurrentWriterIndex() {
        return currentWriterIndex;
    }

    public void setCurrentWriterIndex(int currentWriterIndex) {
        this.currentWriterIndex = currentWriterIndex;
    }

    public int getAfterWriteIndex() {
        return afterWriteIndex;
    }

    public void setAfterWriteIndex(int afterWriteIndex) {
        this.afterWriteIndex = afterWriteIndex;
    }



}
