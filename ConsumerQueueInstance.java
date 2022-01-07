package com.zheng.store;

import com.zheng.model.msg.Message;
import com.zheng.parallel.NamedThreadFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zheng
 */
public class ConsumerQueueInstance {
    public static final String configPath0 = System.getProperty("user.dir")+File.separator+"src"+File.separator+"main"+File.separator;

    public static final String DEFAULT_DATA_PATH = "data";
    public static final String DATA_BACKUP_PATH = "/backup";
//    private static final BlockingQueue<String> QUEUE = new LinkedBlockingQueue<>();
//    private Map<String,ConsumerQueue> consumerQueueMap;
    private ConsumerQueue consumerQueue;
    private AtomicLong count = new AtomicLong(0);
    public Long getCount(){
        return count.get();
    }
    public Message read(){
        try {
            return consumerQueue.read();
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }

    }
    public void writeMessgae(String message){
        count.incrementAndGet();
        consumerQueue.offer(message.getBytes());

    }
    public void writeMessge(byte[] datas){
        consumerQueue.write(datas);
    }
    public void writeMessage(Message message){
        consumerQueue.wirte(message);
    }
    public ConsumerQueue getConsumerQueue() {
        return consumerQueue;
    }

    public void setConsumerQueue(ConsumerQueue consumerQueue) {
        this.consumerQueue = consumerQueue;
    }

    private ScheduledExecutorService executorService;
    public static ConsumerQueueInstance INSTANCE = null;
    private String filePath = null;

    public byte[] readyData(Message message) {
        return consumerQueue.readyData(message);
    }

    private static class ConsumerQueuePoolHolder{
        private static ConsumerQueueInstance pool = new ConsumerQueueInstance();
    }
    public static ConsumerQueueInstance getInstance(){
        return ConsumerQueuePoolHolder.pool;
    }
    private ConsumerQueueInstance(){
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
        consumerQueue = new ConsumerQueue("first",filePath);

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
