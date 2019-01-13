package com.ucp.kafkastream.manager;

import com.ucp.kafkastream.dto.BaseConsumer;
import com.ucp.kafkastream.dto.consumerdto.ConsumerThreadHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class ConsumerManager extends BaseConsumer {
    private final Logger LOG= LoggerFactory.getLogger(ConsumerManager.class) ;
    private ThreadPoolExecutor executor =null ;
    private int threadNumer ;
    private static  ConsumerManager _instance = null ;
    private ConsumerThreadHandle kafkaConsumer = null ;
    private String PROPERTIES_PATH = "" ;
    private ConsumerManager(){
        init();
    }


    public static ConsumerManager getInstance() {
        if(_instance == null){
            synchronized (ConsumerManager.class){
                if(_instance == null){
                    _instance = new ConsumerManager() ;
                }
            }
        }
        return _instance;
    }

    public void init() {
        File file = new File(PROPERTIES_PATH) ;
        InputStream input = null;
        try {
            input = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        Properties props = new Properties() ;
        try {
            props.load(input) ;
        } catch (IOException e) {
            e.printStackTrace();
        }
        kafkaConsumer = new ConsumerThreadHandle(props);
        threadNumer = Integer.parseInt(props.getProperty("MaxThreadSize")) ;
        this.executor = new ThreadPoolExecutor(threadNumer,
                threadNumer,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(threadNumer),
                new ThreadPoolExecutor.CallerRunsPolicy()) ;
    }

    public int getThreadNumer() {
        return threadNumer;
    }

    public int getThreadPoolMaxIdleCount(){
        int activate = executor.getActiveCount() ;
        return threadNumer - activate ;
    }

    public ThreadPoolExecutor getExecutor(){
        return executor ;
    }

    public ConsumerThreadHandle getKafkaConsumer() {
        return kafkaConsumer;
    }

}
