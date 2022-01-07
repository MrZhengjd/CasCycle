package com.zheng.store;

import sun.misc.Contended;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zheng
 */
public class DefaultPageIndex implements PageIndex {
//    private volatile long v1,v2,v3,v4,v5,v6,v7;
    @Contended
    private volatile int readPosition;   // 12   读索引位置
    @Contended
    private volatile int writePosition;  // 24  写索引位置

    private static final String INDEX_FILE_SUFFIX = ".aoq";
    private volatile int indexPage;
//    private AtomicInteger readerIndex = new AtomicInteger(0);
//    private AtomicInteger writerIndex = new AtomicInteger(0);
//    private AtomicBoolean holdRead = new AtomicBoolean(false);
//    private AtomicBoolean holdWrite = new AtomicBoolean(false);
    private RandomAccessFile accessFile;

    private FileChannel fileChannel;

    //读写分离
    private MappedByteBuffer index;

    public DefaultPageIndex(String queueName,String fileDir,int pageIndex){
        this.indexPage = pageIndex;
        pageIndex = pageIndex % UnLockQueue.MAX_BLOCK;
        String indexFilePath = formatPageIndexFilePath(queueName,fileDir,pageIndex);

        File file = new File(indexFilePath);
        try {
            if (file.exists()){
                this.accessFile = new RandomAccessFile(file,"rw");
                byte[] bytes = new byte[8];

                this.accessFile.read(bytes,0,8);
                String s = new String(bytes);
                if (!VERSION.equals(s)){
                    throw new IllegalArgumentException("version dont match");
                }
                this.indexPage = pageIndex;
                this.readPosition = accessFile.readInt();
                this.writePosition = accessFile.readInt();

                this.fileChannel = accessFile.getChannel();
                this.index = fileChannel.map(FileChannel.MapMode.READ_WRITE,0,INDEX_SIZE);
                this.index = index.load();
            }else {
                this.accessFile = new RandomAccessFile(file,"rw");
                this.fileChannel = accessFile.getChannel();
                this.index = fileChannel.map(FileChannel.MapMode.READ_WRITE,0,INDEX_SIZE);
                setVersion();
                setReadPosition(0);
                setWriterPosition(0);
                setIndex(indexPage);

            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static String formatPageIndexFilePath(String queueName, String fileBackupDir,int index) {
        return fileBackupDir + File.separator + String.format("pindex_%s%d%s", queueName,index, INDEX_FILE_SUFFIX);
    }



    @Override
    public void setVersion() {
        index.position(0);
        index.put(VERSION.getBytes());
    }

    @Override
    public int getReadPosition() {
        return readPosition;
    }

    @Override
    public void setIndex(int index) {
        this.indexPage = index;
//        this.index.position(INDEX);
//        this.index.putInt(index);
        this.index.putInt(INDEX,index);
    }

    @Override
    public int getIndex() {
        return indexPage;
    }

    @Override
    public int getWriterPosition() {
        return writePosition;
    }

    @Override
    public void setReadPosition(int readPosition) {
        index.putInt(READ_POSITION,readPosition);
//        index.position(READ_POSITION);
//        index.putInt(readPosition);
        this.readPosition = readPosition;
    }

    @Override
    public void setWriterPosition(int writerPosition) {
        index.putInt(WRITER_POSITION,writerPosition);
//        index.position(WRITER_POSITION);
//        index.putInt(writerPosition);
        this.writePosition = writerPosition;
    }

    @Override
    public void sync() {
        if (index != null){
            index.force();
//            System.out.println("here is page index sync()-----------------------");
//            index.position(0);
        }
    }

    @Override
    public void setByIndex(QueueIndex index) {
        setReadPosition(index.getReadPosition());
        setWriterPosition(index.getWriterPosition());
    }

    @Override
    public void close() {
        try {
            if (index == null) {
                return;
            }
            sync();
//

            index = null;
            accessFile = null;
            fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void load() {
        if (index != null){
            index.load();
        }
    }
}
