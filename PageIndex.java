package com.zheng.store;

/**
 * @author zheng
 */
public interface PageIndex {
    String VERSION = "page.1.0";
    int INDEX_SIZE = 20;
    int READ_POSITION = 8;
    int WRITER_POSITION = 12;
    int INDEX = 16;

    void setVersion();

    int getReadPosition();

    void setIndex(int index);

    int getIndex();

    int getWriterPosition();

    void setReadPosition(int readPosition);

    void setWriterPosition(int writerPosition);

    void sync();

    void setByIndex(QueueIndex index);

    void close();

    void load();
}
