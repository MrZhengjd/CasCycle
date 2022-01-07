package com.zheng.store;

import com.zheng.model.msg.Message;
import com.zheng.parallel.NamedThreadFactory;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zheng
 */
@Slf4j
public class UnLockQueueInstance {
    private static Logger logger = LoggerFactory.getLogger(UnLockQueueInstance.class);
    public static final String configPath0 = System.getProperty("user.dir")+File.separator+"src"+File.separator+"main"+File.separator;

    public static final String DEFAULT_DATA_PATH = "data";
    public static final String DATA_BACKUP_PATH = "/backup";
//    private static final BlockingQueue<String> QUEUE = new LinkedBlockingQueue<>();
//    private Map<String,ConsumerQueue> consumerQueueMap;
    private AtomicInteger seqId = new AtomicInteger(0);
    private UnLockQueue consumerQueue;
    private Long count = 0l;
    private AtomicLong comming = new AtomicLong(0);
    private long successCount = 0;
    private AtomicLong readSucessCount = new AtomicLong(0);
    private TempResult last;
    private long fialedCount = 0;
    public Long getComming(){
        return consumerQueue.getComming();
    }
    public Long getCount(){
        return consumerQueue.getReadingCount();
    }
//    public Long getNullCount(){
//        return consumerQueue.getNutNullCount();
//    }
//    public Message read(){
//        try {
//            return consumerQueue.read();
//        }catch (Exception e){
//            e.printStackTrace();
//            return null;
//        }
//
//    }
    public void writeMessgae(String message){

        consumerQueue.offer(message.getBytes());

    }

    public long getFialedCount() {
        return fialedCount;
    }

    public int readPage(){
        return consumerQueue.getCurrentReadPage();
    }
    public int writePage(){
        return consumerQueue.getCurrentWritePage();
    }

    public long getSuccessCount() {
        return successCount;
    }

    public UnLockQueue getConsumerQueue() {
        return consumerQueue;
    }

    public void setConsumerQueue(UnLockQueue consumerQueue) {
        this.consumerQueue = consumerQueue;
    }

    private ScheduledExecutorService executorService;
    public static UnLockQueueInstance INSTANCE = null;
    private String filePath = null;

    public byte[] readyData(Message message) {
        message.getHeader().setSeqId(seqId.getAndIncrement());
        return consumerQueue.readyData(message);
    }
    public void writeMessge(byte[] datas){
        boolean write = consumerQueue.write(datas);
        if (write){
            successCount ++;
        }else {
            fialedCount ++;
        }
        comming.incrementAndGet();
    }

    public void writeMessage(Message message) {
//        System.out.println("here is seq id "+message.getHeader().getSeqId());
        consumerQueue.write(message);
    }

    public long getReadSucessCount() {
        return readSucessCount.get();
    }

//    public long getNotNullCount(){
//        return consumerQueue.getNutNullCount();
//    }
    public void read() {
//        Long start = System.currentTimeMillis();
//        byte[] datas = consumerQueue.poll();
        TempResult result = consumerQueue.poll2();
        if (result != null){
//            if (last == null){
//                last = result;
//                readSucessCount.getAndIncrement();
//            }else {
//                if (last.getReaderPosition() != result.getReaderPosition()){
//                    readSucessCount.getAndIncrement();
//                }
//                last = result;
//            }
//            Message message = consumerQueue.readDataToMessage(result);
////
//            if (!message.getBody().toString().equals("")){
//                readSucessCount.getAndIncrement();
//            }
//            logger.info(message.toString());
        }
//        Long endTime = System.currentTimeMillis();

//        count++;
//        Message message = consumerQueue.readDataToMessage(result);
//        if (message == null){
//            fialedCount++;
//        }
//        if (count< 100)
//        if ((endTime - start) > 10)
//        log.info("message "+message);
    }

    private static class ConsumerQueuePoolHolder{
        private static UnLockQueueInstance pool = new UnLockQueueInstance();
    }
    public synchronized static UnLockQueueInstance getInstance(){
        return ConsumerQueuePoolHolder.pool;
    }
    private UnLockQueueInstance(){
        this.filePath = configPath0 + DEFAULT_DATA_PATH;
        File file = new File(filePath);
        if (!file.exists()){
            file.mkdirs();
        }
        if (!file.isDirectory() || !file.canRead()){
            throw new IllegalArgumentException("arguement not wright-------------");
        }
        File backup = new File(this.filePath + DATA_BACKUP_PATH);
        if (!backup.exists()){
            backup.mkdirs();
        }
        if (!backup.isDirectory() || !backup.canRead()){
            throw new IllegalArgumentException("arguement not wright-------------");
        }
        consumerQueue = new UnLockQueue("first",filePath);

        executorService = Executors.newScheduledThreadPool(1,new NamedThreadFactory());
        executorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                consumerQueue.sync();
//                deleteBlockFile();
            }


        },1000l,1000L, TimeUnit.MILLISECONDS);

    }



}
