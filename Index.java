package com.zheng.store;

/**
 * @author zheng
 */
public interface Index {
    String VERSION = "page.1.0";
    int INDEX_SIZE = 24;
    int READ_INDEX = 8;
    int WRITE_INDEX = 16;
    void setReadIndex(long readIndex);
    void setWriteIndex(long writeIndex);
    long getReadIndex();
    long getWriteIndex();
    void setVersion();
}
