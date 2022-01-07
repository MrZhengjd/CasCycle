package com.zheng.store;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author zheng
 */
public class DefaultQueueIndex implements QueueIndex {
    private volatile long v1,v2,v3,v4,v5,v6,v7;
    private volatile int readPosition;   // 12   读索引位置

    private volatile int writePosition;  // 24  写索引位置

    private volatile int readPage; //读的页数索引

    private volatile int writePage;//写的页数索引

    private static final String INDEX_FILE_SUFFIX = ".aoq";


    private RandomAccessFile accessFile;

    private FileChannel fileChannel;

    //读写分离
    private MappedByteBuffer index;

    public static String formatIndexFilePath(String queueName, String fileBackupDir) {
        return fileBackupDir + File.separator + String.format("tindex_%s%s", queueName, INDEX_FILE_SUFFIX);
    }

    public DefaultQueueIndex(String queueName, String fileDir, int operationType){

        String indexFilePath = formatIndexFilePath(queueName,fileDir);
        if (!indexFilePath.contains(INDEX_FILE_SUFFIX)){
            System.out.println("here is the suffix not contain "+indexFilePath);
        }
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
//
                this.readPage = accessFile.readInt();

                this.writePage = accessFile.readInt();
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
                setReadPage(0);
                setWritePage(0);

            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private MappedByteBuffer byteBuffer;

    @Override
    public void setReadPage(int readPage) {
        index.position(READ_PAGE);
        index.putInt(readPage);
        this.readPage = readPage;
    }

    @Override
    public void setWritePage(int writePage) {
        index.position(WRITE_PAGE);
        index.putInt(writePage);
        this.writePage = writePage;
    }

    @Override
    public int getWritePage() {
        return writePage;
    }

    @Override
    public int getReadPage() {
        return readPage;
    }

    @Override
    public void setVersion() {
        this.index.position(0);
        this.index.put(VERSION.getBytes());
    }

    @Override
    public int getReadPosition() {
        return readPosition;
    }

    @Override
    public int getWriterPosition() {
        return writePosition;
    }



    @Override
    public void setReadPosition(int readPosition) {
//        System.out.println("here is set reader position "+readPosition);
        if (readPosition > QueueBlock.BLOCK_SIZE){
            readPosition = QueueBlock.BLOCK_SIZE;
        }
        this.index.position(READ_POSITION);
        this.index.putInt(readPosition);
        this.readPosition = readPosition;
    }

    @Override
    public void setWriterPosition(int writerPosition) {
        if (writerPosition > QueueBlock.BLOCK_SIZE){
            writerPosition = QueueBlock.BLOCK_SIZE;
        }
        this.index.position(WRITER_POSITION);
        this.index.putInt(writerPosition);
        this.writePosition = writerPosition;
    }





    @Override
    public void reset() {
        int size = writePosition - readPosition;
        setReadPosition(0);
        setWriterPosition(size);
        if (size == 0 && readPosition == writePosition){
            setReadPosition(0);
            setWriterPosition(0);
        }
    }

    @Override
    public void sync() {
        if (index != null){
            index.force();
//            index.position(0);
        }
    }

    @Override
    public void close() {

    }
}
