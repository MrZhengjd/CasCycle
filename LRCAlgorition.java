package com.zheng.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Contended;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zheng
 */
public class LRCAlgorition<K extends Object,V extends Object> {
    private static Logger logger = LoggerFactory.getLogger(LRCAlgorition.class);
    private Map<K,Node<K,V>> nodeMap = new HashMap<>();
    @Contended
    private volatile int size;
    private static int capacity;
    @Contended
    private volatile int count;
    private Node head;
    private Node tail;
    public  void changeSize(int operateSize){
        this.size = this.size + operateSize;
        if (operateSize>0){
            count ++;
        }else {
            count --;
        }
    }
    public LRCAlgorition(int capacity) {
        this.capacity = capacity;
        this.size = 0;
        head = Node.getInstance();
        tail = Node.getInstance();
        head.next = tail;
        tail.prev = head;
    }
    private void rotateFullSpace(boolean rotate,Node node){
        if ( !rotate && size+node.length > capacity ){
            Node tem = tail.prev;
            tem.prev.next = tail;
            tail.prev = tem.prev;
            nodeMap.remove(tem);
            changeSize(-tem.length);
//            logger.info("rotate here -------");
            if (size + node.length > capacity){
                rotateFullSpace(rotate,node);
            }
        }
    }
    private void addHead(Node node,boolean rotate){
        rotateFullSpace(rotate,node);
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;
        if (!rotate){
            changeSize(node.length);
        }

    }
    public void listAll()
    {
        listInfo(head);
    }
    private void listInfo(Node first){
        if (first == null){
            return;
        }
        Node node = first.next;
        if (node == null){
            return;
        }
        if (node != tail){
            logger.info("node "+node.key +" value "+node.value);
            listInfo(node);
        }
    }
    private void removeNode(Node temp){
        Node prev = temp.prev;
        Node next = temp.next;
        prev.next = next;
        next.prev = prev;
        nodeMap.remove(temp.key);
        changeSize(-temp.length);
    }
    public void removeByKey(K key){
        Node temp = nodeMap.get(key);
        if (temp == null){
            return ;
        }
        removeNode(temp);
    }

    public V getNode(K key){
        Node temp = nodeMap.get(key);
        if (temp == null){
            return null;
        }
        rotateFirst(temp);
        return (V) temp.value;

    }

    /**
     * 只有一个数据不翻转
     * @param temp
     */
    private void rotateFirst(Node temp){
        if (count == 1){
            return;
        }
        if (head.next == temp){
            return;
        }
//        removeNode(temp);
        temp.prev.next = temp.next;
        temp.next.prev = temp.prev;
        addHead(temp,true);
//        nodeMap.put((K)temp.key,temp);
    }
    public void put(K key,V value){
        put(key,value,1);

    }
    public void put(K key,V value,int length){
        Node temp = nodeMap.get(key);
        if (temp == null){
            Node node = Node.getInstance();
            node.setInfo(key,value,length);
            //添加到头结点的下一个节点
            addHead(node,false);
            nodeMap.put(key,node);
            return;
        }
        rotateFirst(temp);

    }

    static class Node<K,V> implements Serializable,Cloneable {
        static class Holder{
            private static Node INSTANCE = new Node();
        }
        public static Node getInstance(){
            try {
                return (Node) Holder.INSTANCE.clone();
            }catch (Exception e){
                e.printStackTrace();
            }
            return Holder.INSTANCE;
        }
        public Node() {
        }
        int length;
        K key;
        V value;
        Node next;
        Node prev;
        public void setInfo(K key, V value, int length){
            this.key = key;
            this.value = value;
            this.length = length;
        }
        public Node(K key, V value,int length) {
            this.key = key;
            this.value = value;
            this.next = null;
            this.prev = null;
            this.length = length;
        }
    }
}
