package com.zheng.store;

import io.netty.buffer.ByteBuf;

import java.io.Serializable;

/**
 * @author zheng
 */
public class TempResult implements Serializable,Cloneable {
    private int state;
    private byte[] datas;
    private int readerPosition;
    private ByteBuf byteBuf;

    public ByteBuf getByteBuf() {
        return byteBuf;
    }

    public void setByteBuf(ByteBuf byteBuf) {
        this.byteBuf = byteBuf;
    }

    public void setDatas(byte[] datas) {
        this.datas = datas;
    }
    private static class Holder{
        private static TempResult INSTANCE = new TempResult();
    }
    public static TempResult getInstance(){
        return Holder.INSTANCE;
//        try {
//            return (TempResult) Holder.INSTANCE.clone();
//        } catch (CloneNotSupportedException e) {
//            e.printStackTrace();
//        }
//        return Holder.INSTANCE;
    }


    public TempResult() {
        super();
    }

    public void init(){
        state = 0;

        readerPosition = 0;
    }
    public int getReaderPosition() {
        return readerPosition;
    }

    public void setReaderPosition(int readerPosition) {
        this.readerPosition = readerPosition;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public byte[] getDatas() {
        return datas;
    }



}
