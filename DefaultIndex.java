package com.zheng.store;

import sun.misc.Contended;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * @author zheng
 */
public class DefaultIndex implements Index {

    @Contended
    private volatile long readIndex;   // 12   读索引位置
    @Contended
    private volatile long writeIndex;  // 24  写索引位置

    private static final String INDEX_FILE_SUFFIX = ".index";

    private RandomAccessFile accessFile;

    private FileChannel fileChannel;

    //读写分离
    private MappedByteBuffer index;

    public DefaultIndex(String queueName,String fileDir,int pageIndex){

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
                this.readIndex = accessFile.readLong();
                this.writeIndex = accessFile.readLong();
                this.fileChannel = accessFile.getChannel();
                this.index = fileChannel.map(FileChannel.MapMode.READ_WRITE,0,INDEX_SIZE);
                this.index = index.load();
            }else {
                this.accessFile = new RandomAccessFile(file,"rw");
                this.fileChannel = accessFile.getChannel();
                this.index = fileChannel.map(FileChannel.MapMode.READ_WRITE,0,INDEX_SIZE);
                setVersion();
                setReadIndex(0l);
                setWriteIndex(0l);

            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static String formatPageIndexFilePath(String queueName, String fileBackupDir,int index) {
        return fileBackupDir + File.separator + String.format("pindex_%s%d%s", queueName,index, INDEX_FILE_SUFFIX);
    }

    @Override
    public void setReadIndex(long readIndex) {
        index.putLong(READ_INDEX,readIndex);
        this.writeIndex = readIndex;

    }

    @Override
    public void setWriteIndex(long writeIndex) {
        index.putLong(WRITE_INDEX,writeIndex);
        this.writeIndex = writeIndex;

    }

    @Override
    public long getReadIndex() {
        return readIndex;
    }

    @Override
    public long getWriteIndex() {
        return writeIndex;
    }

    @Override
    public void setVersion() {
        index.position(0);
        index.put(VERSION.getBytes());
    }
}
