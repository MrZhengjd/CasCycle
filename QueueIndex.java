package com.zheng.store;

/**
 * @author zheng
 */
public interface QueueIndex {
    String VERSION = "disk.1.0";
    int INDEX_SIZE = 24;
    int READ_POSITION = 16;
    int WRITER_POSITION = 20;
    int READ_PAGE = 8;
    int WRITE_PAGE = 12;


    void setReadPage(int readPage);

    void setWritePage(int writePage);

    int getWritePage();

    int getReadPage();
    void setVersion();

    int getReadPosition();

    int getWriterPosition();

    void setReadPosition(int readPosition);

    void setWriterPosition(int writerPosition);



    void reset();

    void sync();

    void close();
}
