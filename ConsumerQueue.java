package com.zheng.store;

import com.zheng.model.msg.Message;
import com.zheng.parallel.SegmentReadLock;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.util.AbstractQueue;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

/**
 * @author zheng
 * 读写锁获取内容
 *
 * 通过readBlockIndex和writeBlcokIndex知道当前位置
 */
public class ConsumerQueue extends AbstractQueue<byte[]> {
    private volatile long v1,v2,v3,v4,v5,v6,v7;
//    private volatile Thread readingThread;
    private volatile int readings = 0;
//    private AtomicInteger readings = new AtomicInteger(0);
    private volatile Thread writingThread;
    private volatile int writtings = 0;
//    private AtomicInteger writtings = new AtomicInteger(0);
    private AtomicInteger currentWriteIndex = new AtomicInteger(0);
    private AtomicInteger currentReaderIndex = new AtomicInteger(0);
//    private volatile int readBlockIndex;
//    private volatile int writeBlcokIndex;
//    private volatile int readPosition;
//    private volatile int writePosition;
    private static final int maxBlock = 128;
    private AtomicBoolean hold = new AtomicBoolean(false);
    private boolean single = false;
//    private AtomicBoolean single = new AtomicBoolean(false);
    private PooledByteBufAllocator allocator = new PooledByteBufAllocator();
    private String name;
    private String dir;
    private QueueIndex readwriterPageIndex;
//    private QueueIndex readerIndex;
//    private ReentrantLock readLock ;
    private SegmentReadLock segmentReadLock = new SegmentReadLock();
    private ReentrantReadWriteLock writeLock;
    private StampedLock stampedLock = new StampedLock();
    private Map<Thread,Integer> readingMap = new HashMap<>();


    private QueueBlock writeBlock;
    private QueueBlock[] blocks;
    private PageIndex readPage;
    private PageIndex writePage;
    private QueueBlock readBlock;
    private AtomicInteger size;
    private AtomicInteger coming = new AtomicInteger(0);
    private AtomicInteger out = new AtomicInteger(0);

    public void setWritePage(PageIndex writePage) {
        this.writePage = writePage;
    }

    public QueueIndex getWriterIndex() {
        return readwriterPageIndex;
    }

    public void setWriterIndex(QueueIndex writerIndex) {
        this.readwriterPageIndex = writerIndex;
    }

    public static final String BLOCK_FILE_SUFFIX = ".log";//数据文件
    public ConsumerQueue(String name, String dir ) {
        this.name = name;
        this.dir = dir;
        this.readwriterPageIndex = new DiskQueueIndex(name,dir,0);
        currentReaderIndex.set(readwriterPageIndex.getReadPosition());
        currentWriteIndex.set(readwriterPageIndex.getWriterPosition());
//        this.readerIndex = new DiskQueueIndex(name,dir,0);
//        this.readLock = new ReentrantLock(false);
        this.writeLock = new ReentrantReadWriteLock(false);
        this.size = new AtomicInteger(0);
//        readBlockIndex = readerIndex.getReadPage();
//        readBlockIndex = 0;
//        writeBlcokIndex = writerIndex.getWritePage();
        writePage = new DefaultPageIndex(name,dir,0);
        readPage = new DefaultPageIndex(name,dir,0);
        String filePath = formatBlockFilePath(name,0,dir);
        writeBlock = new QueueBlock(writePage,readPage,filePath);
        readBlock = new QueueBlock(writePage,readPage,formatBlockFilePath(name,readPage.getIndex(),dir));


        blocks = new QueueBlock[maxBlock];

        blocks[writePage.getWriterPosition()%maxBlock] = writeBlock;
    }
    public static String formatBlockFilePath(String queueName, int fileNum, String fileBackupDir) {
        return fileBackupDir + File.separator + String.format("tblock_%s_%d%s", queueName, fileNum, BLOCK_FILE_SUFFIX);
    }
    @Override
    public Iterator<byte[]> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return this.size.get();
    }
    public int getComming(){
        return coming.get();
    }

    public int getHandle(){
        return out.get();
    }
    @Override
    public boolean offer(byte[] bytes) {

        if (ArrayUtils.isEmpty(bytes)) {
//            System.out.println("is empty ----------------------");
            return true;
        }
        rotateOfferMessage(bytes);
        coming.incrementAndGet();
//            single = false;
        hold.set(false);
        return true;

//        while (hold.compareAndSet(false, true)) {
//            rotateOfferMessage(bytes);
//            coming.incrementAndGet();
////            single = false;
//            hold.set(false);
//            return true;
//        }
//        int writer = currentWriteIndex.get();
//        if (currentReaderIndex.get() == currentWriteIndex.get()) {
////                System.out.println("is same state and comming to write ");
////            long stamp = stampedLock.writeLock();
////            writeLock.writeLock().lock();
//            while (hold.compareAndSet(false, true)) {
//                rotateOfferMessage(bytes);
//                coming.incrementAndGet();
//                single = false;
//                hold.set(false);
//                return true;
//            }
//
//
////            segmentReadLock.readLock(writer);
////            try {
////                rotateOfferMessage(bytes);
////                coming.incrementAndGet();
////                single = false;
////                writingThread = null;
//////                writtings--;
////                return true;
////                boolean firstSet = false;
////                if (writingThread == null){
////                    writingThread = Thread.currentThread();
////                    firstSet = true;
////                }
////
////                while (true){
////
////                    if (readings > 0){
////                        continue;
////                    }
////                    boolean canWrite = false;
////
////                    if (!firstSet && Thread.currentThread() == writingThread){
////                        canWrite = true;
////                    }else if (firstSet){
////                        canWrite = true;
////                    }
////
//////                    if (!canWrite && single.compareAndSet(false,true)){
//////                        canWrite = true;
//////                    }
//////
////                    if (!canWrite){
////                        continue;
////                    }
////                    writtings++;
//////                        writingThread = Thread.currentThread();
////                    rotateOfferMessage(bytes);
////                    coming.incrementAndGet();
////                    single = false;
////                    writingThread = null;
////                    writtings--;
////                    return true;
////                }
////            }catch (Exception e){
////                e.printStackTrace();
////                return false;
////            }finally {
//////                writeLock.writeLock().unlock();
//////                stampedLock.unlockWrite(stamp)
////                segmentReadLock.unReadlock(writer);
////            }
//
//
//        } else {
////                writingThread = Thread.currentThread();
////                writtings --;
//            while (hold.compareAndSet(false, true)) {
//                rotateOfferMessage(bytes);
//                coming.incrementAndGet();
//                hold.set(false);
//                return true;
////                LockSupport.park();
//            }
//
//
////            segmentReadLock.readLock(writer);
////            try {
////                rotateOfferMessage(bytes);
////                coming.incrementAndGet();
////                return true;
////            }catch (Exception e){
////                e.printStackTrace();
////                return false;
////            }finally {
////                segmentReadLock.unReadlock(writer);
////            }
//
//
//        }


//            System.out.println("here is comming offer -----------");
//            offerMessage(bytes);

//            if (writeBlcokIndex == 999){
//                System.out.println("here last writeblock index "+writeBlcokIndex + " and read block index "+readBlockIndex);
//            }


//        return false;
    }
    /**
     * 加入数据
     * @param bytes
     */
    public void rotateOfferMessage(byte[] bytes){
        if (!writeBlock.isRotateSpaceAvaibale(bytes.length)){
            nextWriteBlock();
        }
        writeBlock.writeRotate(bytes);
//        System.out.println("here is write at "+writePage.getWriterPosition() + " data length "+bytes.length + " after writer position "+writePosition +" write page "+writePage);
//        writePosition = writeBlock.getWriterIndex().getWriterPosition();
//        if (currentWriteIndex.get() == currentReaderIndex.get()){
////            writeBlock.sync();
//            readPage.setWriterPosition(writePage.getWriterPosition());
////            System.out.println("here is current write at "+writePage.getWriterPosition() + " and read position "+readPage.getReadPosition());
////
//        }
        size.incrementAndGet();
    }
    /**
     * 加入数据
     * @param bytes
     */
    public void offerMessage(byte[] bytes){
        if (!writeBlock.isSpaceAvaibale(bytes.length)){
            nextWriteBlock();
        }
        writeBlock.write(bytes);
        size.incrementAndGet();
    }
    /**
     * 获取下一个block
     * 并且这是一个环
     */
    private void nextWriteBlock() {

//        int nextBlockNum = writePage.getIndex() + 1;

        if ((writePage.getIndex() - readPage.getIndex()) > maxBlock){
            throw new RuntimeException("queue is full load -------------");
        }
//        System.out.println("rotate write here is readblockIndex ");
        writeBlock.putEND();
        int nextBlockNum = currentWriteIndex.incrementAndGet();
//
        readwriterPageIndex.setWritePage(nextBlockNum);
        readPage.setWriterPosition(writePage.getWriterPosition());
        writeBlock.sync();

//        System.out.println("next write position "+writePage.getWriterPosition());
//        writerIndex.sync();
        writePage.close();

        writePage = new DefaultPageIndex(name,dir,nextBlockNum);
        writePage.setWriterPosition(0);
        nextBlockNum = nextBlockNum % maxBlock;
        writeBlock = new QueueBlock(writePage,readPage,formatBlockFilePath(name,nextBlockNum,dir));
//        writerIndex.setWriterPosition(0);
//        System.out.println("here is next write block "+nextBlockNum);
        blocks[nextBlockNum] = writeBlock;
    }
    public Message read(){

//        byte[] datas = poll();
        try {
            byte[] datas = pollRotate();
            out.incrementAndGet();
            if (datas == null){
//                System.out.println("read null-----------------------------");
                return null;
            }
            ByteBuf byteBuf = allocator.directBuffer(datas.length);
            Message data = Message.read(datas, byteBuf);
            byteBuf.release();
//            if (data != null){}
////            System.out.println(" message "+data.getHeader().toString() +" message "+" from readerIndex "+readBlockIndex);
//            else {
////                System.out.println("read data nulll ================");
//            }
            return data;
        }catch (Exception e){
            e.printStackTrace();

            return null;
        }

    }

    /**
     * 旋转弹出数据
     * @return
     */
    public byte[] pollRotate(){
//        readLock.lock();
        int page = currentReaderIndex.get();
        if (currentReaderIndex.get() !=currentWriteIndex.get()){
//                System.out.println("is not the same page --------------------");
            segmentReadLock.readLock(page);
            try {
                return pollData();
            }
            finally {
                segmentReadLock.unReadlock(page);
//
            }


        }else {
//            long stamp = stampedLock.readLock();
//            writeLock.readLock().lock();
            while (hold.compareAndSet(false,true)){
                int tempPosition = readBlock.getReaderIndex().getReadPosition();
                int length = readBlock.getNextLength();
                byte[] datas = readBlock.read(tempPosition,length);

                if (datas != null){
                    size.decrementAndGet();
                }
                hold.set(false);
//
                return datas;

            }


        }
        return null;


    }
    /**
     * 弹出数据
     * @return
     */
    public byte[] pollData(){
        if (readBlock == null){
//            System.out.println("here is reader index "+readBlockIndex);
            readBlock = new QueueBlock(writePage,readPage,formatBlockFilePath(name,currentReaderIndex.get(),dir));
            readBlock.load();
        }
        boolean rotate = false;
        int  length = readBlock.getNextLength();
//        System.out.println("length "+length + " reader position "+readPosition +" writer position "+writePosition);
        if (readBlock.readEnd(readPage.getReadPosition(),length)){
            rotateNextReadBlock();
            rotate = true;
        }


//        if (rotate){
//            while (true){
//                if (!single.get()){
//                    length = readBlock.getNextLength();
//                    break;
//                }
//            }
//        }
        int tempPosition = readBlock.getReaderIndex().getReadPosition();
        try {
            byte[] datas = readBlock.read(tempPosition,length);
            if (datas != null){
                size.decrementAndGet();
            }
            this.readPage.setReadPosition(readBlock.getReaderIndex().getReadPosition());
            return datas;
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("here is read positin "+tempPosition + " and length "+length + " and write position ");
            return null;
        }

    }
    @Override
    public byte[] poll() {
        throw new UnsupportedOperationException("不支持改方法");

    }

    /**
     *
     */
    private void rotateNextReadBlock() {
        if (currentWriteIndex.get() == currentReaderIndex.get()){
            return;
        }
//        readPage.setByIndex(readerIndex);
        readBlock.close();
//        int temp = readPage.getReadPosition();
        int next = currentReaderIndex.incrementAndGet();
        readwriterPageIndex.setReadPage(next);
        int current = next - 1;
        int index = (next - 1) % maxBlock;
        blocks[index] = null;
        current++ ;
//        readwriterPageIndex.setReadPage(readBlockIndex);
        readPage.setIndex(current);
//        readerIndex.setReadPage(readBlockIndex);
        int nextNum = next;
        if (nextNum == currentWriteIndex.get()){
//            System.out.println("here is front current writing -----------");
            readPage.close();
            readPage = new DefaultPageIndex(name,dir,nextNum);
            readPage.setWriterPosition(writePage.getWriterPosition());
//            readerIndex.setReadPosition(readPage.getReadPosition());
//            readerIndex.setWriterPosition(readPage.getWriterPosition());
            readBlock = writeBlock.simpleDuplicate();
            readBlock.setPath(formatBlockFilePath(name,nextNum,dir));
            readBlock.openChannelBuffer(writeBlock,readPage,writePage);
            readBlock.getReaderIndex().setReadPosition(readPage.getReadPosition());
            readBlock.setWriterIndex(writePage);
//            readPosition = readPage.getReadPosition();
            readPage.getReadPosition();
            readBlock.load();
        }else {
            nextNum = nextNum % maxBlock;
            readBlock = blocks[nextNum];
            readPage.close();
            readPage = new DefaultPageIndex(name,dir,readPage.getIndex());

            if (readBlock == null){
                readBlock = new QueueBlock(writePage,readPage,formatBlockFilePath(name,nextNum,dir));
            }
            readBlock.setPath(formatBlockFilePath(name,nextNum,dir));
            readBlock.openChannelBuffer(null,readPage,writePage);
            readBlock.getReaderIndex().setReadPosition(readPage.getReadPosition());
            readPage.setReadPosition(readPage.getReadPosition());
//            readerIndex.setReadPosition(readPage.getReadPosition());
//            readerIndex.setWriterPosition(readPage.getWriterPosition());
//            System.out.println("here is from old writing--------------"+readBlockIndex);
            readBlock.setPath(formatBlockFilePath("first",nextNum,dir) );

            readBlock.getWriterIndex().setWriterPosition(writePage.getWriterPosition());
            readBlock.setWriterIndex(writePage);
//            if (readBlock.checkByteBufferOpen()){
//                System.out.println("here is the reader is null");
//            }
        }
        if (readBlock == null){
            System.out.println("is readblock null-------------");
        }
//        readBlock.getReaderIndex().setReadPosition(0);

    }

    @Override
    public byte[] peek() {
        throw new UnsupportedOperationException();
    }

    public void sync() {
        try {
            writePage.sync();
            readwriterPageIndex.sync();
        }catch (Exception e){
            e.printStackTrace();
        }
        writeBlock.sync();
    }

    public byte[] readyData(Message message){
        ByteBuf byteBuf = allocator.directBuffer();
        message.writeToByteBuf(byteBuf);
        int length = byteBuf.readableBytes();
//        System.out.println("out readable bytes "+byteBuf.readableBytes());
        if (length > 0 ){
//            System.out.println("here is coming ======================");
            byte[] datas = new byte[length];
            byteBuf.readBytes(datas);
            byteBuf.release();
            return datas;
        }
        byteBuf.release();
        return null;
    }
    public boolean write(byte[] data){
        return offer(data);
    }
    public boolean wirte(Message message) {

//        System.out.println("here is writing --------------");
//        System.out.println("here is write a message "+message.getHeader().toString() + " message "+message.getBody().toString());

        ByteBuf byteBuf = allocator.directBuffer();
        message.writeToByteBuf(byteBuf);
        int length = byteBuf.readableBytes();
//        System.out.println("out readable bytes "+byteBuf.readableBytes());
        if (length > 0 ){
//            System.out.println("here is coming ======================");
            byte[] datas = new byte[length];
            byteBuf.readBytes(datas);
            byteBuf.release();
            return offer(datas);
        }
        return false;
    }
}
