package com.ucp.kafkastream.util;

import com.ucp.kafkastream.vo.XmlBean;
import com.ucp.kafkastream.redisdata.RedisStoredInfo;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

public class RedisUtils {
    final static Logger LOG = LoggerFactory.getLogger(RedisUtils.class);
    private static String configFilePath = "./redis.properties";
    private static String configFilePaths = "./configs/redistime.xml" ;
    private String host = "127.0.0.1";
    private int port = 6379;
    private String password = "123456";
    //redis DB
    private int dbIndex = 0;
    //如果redis断开，尝试链接不上，就些本地日志
    private int retryConnectRedisCount = 10;
    //批量写入redis
    private int redisSendCount = 1000;
    //redis重试连接间隔时间
    private int redisReconnect_time = 5000;
    //是否丢弃所有日志
    private AtomicBoolean isAbandonAllLog = new AtomicBoolean(false);
    private String key = "log4j-log";
    //连接超时
    private int timeout = 10000;
    private long minEvictableIdleTimeMillis = 120000L;
    private long timeBetweenEvictionRunsMillis = 30000L;
    private int numTestsPerEvictionRun = -1;
    private int maxTotal = 8;
    private int maxIdle = 0;
    private int minIdle = 0;
    private boolean blockWhenExhaused = false;
    private String evictionPolicyClassName;
    private boolean lifo = false;
    private boolean testOnBorrow = false;
    private boolean testWhileIdle = false;
    private boolean testOnReturn = false;
    private String dateFormatter = "yyyy-MM-dd'T'HH:mm:ss.SSS'+0800'";
    private String logFormatFeature;
    private static AtomicBoolean jedisHeath = new AtomicBoolean(true);
    public static JedisPool jedisPool;
    private static Jedis _write_jedis_01;
    private static Jedis _write_jedis_02;

    private BlockingQueue<Map<String, Object>> redis_msg_container = new LinkedBlockingQueue<Map<String, Object>>(Integer.MAX_VALUE);
    private static ThreadPoolExecutor _executor;
    // 是否写本地，临时变量
    private static AtomicBoolean isWriteLocal = new AtomicBoolean(false);
    // 配置文件上次修改时间
    private static long configFileLastModifyTime = 0L;
    private final static String nextLineChar = System.getProperty("line.separator");
    private static RedisUtils _instance = null;
    private static AtomicInteger _sendCount = new AtomicInteger(0) ;
    private static HashMap<String,XmlBean> maps = null ;

    private RedisUtils() {
        activateOptions();
        XmlLoadToMapUtil xmlLoadToMapUtil = XmlLoadToMapUtil.getInstance() ;
        xmlLoadToMapUtil.parseXml(configFilePaths) ;
        xmlLoadToMapUtil.getXmlMap() ;
    }

    public BlockingQueue<Map<String, Object>> getRedis_msg_container() {
        return redis_msg_container;
    }

    public void setRedis_msg_container(BlockingQueue<Map<String, Object>> redis_msg_container) {
        this.redis_msg_container = redis_msg_container;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getDbIndex() {
        return dbIndex;
    }

    public boolean isTestWhileIdle() {
        return testWhileIdle;
    }

    public String getDateFormatter() {
        return dateFormatter;
    }

    public static RedisUtils getInstance() {
        if (_instance == null) {
            _instance = new RedisUtils();
        }
        return _instance;
    }

    public void activateOptions() {
        _instance.activateOptions();
        try {
            initConfig();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        initRedis();
        // Jedis 连接守护线程
        Runnable jedisProtector = createJedisProtectorRunner();
        getExecutor().execute(jedisProtector);
        Runnable handler = createMsgToRedisHandlerRunner();
        getExecutor().execute(handler);
        // 增加打印队列里数据笔数的线程
        Runnable caculateRunner = new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.currentThread().sleep(3000);
                        System.out.println("redis_msg_container size is : " + redis_msg_container.size());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        getExecutor().execute(caculateRunner);
        // 通过增加JVM虚拟机关闭钩子，在JVM退出之前，将缓存中日志发送出去
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                long shutDownTimeSpan = System.currentTimeMillis();// 记录开始关闭时的时间戳
                while (redis_msg_container.size() > 0) {
                    try {
                        if (System.currentTimeMillis() - shutDownTimeSpan > 5000) {
                            // 等待发送线程处理，最多等待5秒后，直接退出
                            break;
                        }
                        Thread.currentThread().sleep(100);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    /*
     * 创建jedis连接守护进程
     */
    private Runnable createJedisProtectorRunner() {
        Runnable jedisProtector = new Runnable() {
            @Override
            public void run() {
                int tryCount = 0;
                while (true) {
                    try {
                        Thread.currentThread().sleep(redisReconnect_time);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        // 每隔 redis_reconnect_time 秒检测一次jedis的健康状态
                        if (!jedisHeath.get()) {
                            initRedis();
                            tryCount++;
                            if (tryCount >= retryConnectRedisCount && !jedisHeath.get()) {
                                isWriteLocal.set(true);
                                redis_msg_container.clear();// 清空队列
                            }
                        }
                        else {
                            tryCount = 0;
                            isWriteLocal.set(false);
                            try {
                                getWriteJedis_02().ping();
                            }
                            catch (JedisConnectionException e) {
                                jedisHeath.set(false);
                                throw e;
                            }
                            // 测试Redis是否连接正常，但是福建农信那边ping会报超时，不知何故，先注释
                        }

                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        return jedisProtector;
    }
    private Runnable createMsgToRedisHandlerRunner() {
        Runnable runner = new Runnable() {
            @Override
            public void run() {
                final AtomicInteger currentCount = new AtomicInteger(0);
                final AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());
                final CopyOnWriteArrayList<Map> msgList = new CopyOnWriteArrayList<>();
                Runnable checkPipeRunner = new Runnable() {
                    @Override
                    public void run() {
                        Jedis jedis = null;
                        Pipeline pipe = null ;
                        String serviceId_xpath = "/sdo/SYS_HEAD/ServiceID".toLowerCase() ;
                        SimpleDateFormat format_yyyyMMdd = new SimpleDateFormat( "yyyy-MM-dd") ;
                        SimpleDateFormat format_yyyyMMddHHmmss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") ;
                        while (true) {
                            try {
                                if (!jedisHeath.get() || isAbandonAllLog.get()) {
                                    Thread.currentThread().sleep(redisReconnect_time);
                                    continue;
                                }
                                if (jedis == null) {
                                    jedis = getWriteJedis_01();
                                    pipe = jedis.pipelined();
                                }
                                if (currentCount.get() >= redisSendCount
                                        || (System.currentTimeMillis() - lastTime.get() >= 4000 )
                                        || (System.currentTimeMillis() - lastTime.get() >= 5000 )) {
                                    int msgSize = msgList.size();
                                    if (msgSize > 0) {
                                        for (Map<String,Object> msg : msgList) {
                                            for (Map.Entry<String,Object> entry:msg.entrySet()) {
                                                byte[] redis_key = SerializeUtl.ObjToSerialize(entry.getKey()) ;
                                                byte[] redis_value = SerializeUtl.ObjToSerialize(entry.getValue()) ;
                                                pipe.set(redis_key,redis_value);

                                                //数据有效期
                                                String keys = String.valueOf(SerializeUtl.deSerialize(redis_key));
                                                String serviceid = keys.split("###")[1] ;
                                                String time = maps.get(serviceid).getTimes() ;
                                                Integer times = Integer.parseInt(time) ;
                                                Integer timeSeconds = times * 86400 ;
                                                String flag = maps.get(serviceid).getFlag() ;
                                                if(flag.equals("1")){
                                                    pipe.expire(redis_key,timeSeconds) ;
                                                } else if(flag.equals("2")){
                                                    //插入数据的当前时间
                                                    SimpleDateFormat sd = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ) ;
                                                    String timeFormat = sd.format(new Date()) ;
                                                    Date startTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timeFormat) ;
                                                    //清理数据时间
                                                    Calendar calendar = Calendar.getInstance(TimeZone.getDefault()) ;
                                                    calendar.add(Calendar.DAY_OF_WEEK,1);
                                                    String timeFormats = format_yyyyMMdd.format(calendar.getTime()) ;
                                                    String cleanTime = timeFormats + maps.get(serviceid).getCleanTime() ;
                                                    Date endTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(cleanTime) ;
                                                    long betweenLongTime = endTime.getTime() - startTime.getTime();
                                                    int secondExpire = Integer.parseInt(String.valueOf( betweenLongTime / 1000 ) );
                                                    pipe.expire(keys,secondExpire) ;
                                                }
                                                LOG.debug("send redis count:" + _sendCount.addAndGet(1));
                                            }
                                        }
                                        pipe.sync();
                                        msgList.clear();
                                        currentCount.set(0);
                                    }
                                    lastTime.set(System.currentTimeMillis());
                                } else {
                                    Thread.currentThread().sleep(1);
                                }
                            } catch (JedisConnectionException e) {
                                jedisHeath.set(false);
                                jedis = null;
                                pipe = null;
                                e.printStackTrace();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                };
                getExecutor().execute(checkPipeRunner);
                while (true) {
                    try {
                        if (!jedisHeath.get() || isAbandonAllLog.get()) {
                            Thread.currentThread().sleep(redisReconnect_time);
                            continue;
                        }
                        if (currentCount.get() < redisSendCount) {
                            Map msg = redis_msg_container.take();
                            if (null != msg && !"".equals(msg)) {
                                msgList.add(msg);
                                currentCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        return runner;
    }

    private void initRedis() {
        closeJedisInstance(_write_jedis_01);
        closeJedisInstance(_write_jedis_02);
        if (jedisPool != null) {
            jedisPool.close();
        }
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        if (lifo) {
            // Redis是否启用后进先出，默认true
            poolConfig.setLifo(lifo);
        }
        if (testOnBorrow) {
            // 在获取连接的时候检查有效性, 默认false
            poolConfig.setTestOnBorrow(testOnBorrow);
        }
        if (isTestWhileIdle()) {
            // 在空闲时检查有效性, 默认false
            poolConfig.setTestWhileIdle(isTestWhileIdle());
        }
        if (testOnReturn) {
            poolConfig.setTestOnReturn(testOnReturn);
        }
        if (timeBetweenEvictionRunsMillis > 0) {
            // 逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
            poolConfig.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        }
        if (evictionPolicyClassName != null && evictionPolicyClassName.length() > 0) {
            // 设置的逐出策略类名, 默认DefaultEvictionPolicy(当连接超过最大空闲时间,或连接数超过最大空闲连接数)
            poolConfig.setEvictionPolicyClassName(evictionPolicyClassName);
        }
        if (blockWhenExhaused) {
            // 连接耗尽时是否阻塞, false报异常,true阻塞直到超时, 默认true
            poolConfig.setBlockWhenExhausted(blockWhenExhaused);
        }
        if (minIdle > 0) {
            // 最小空闲连接数, 默认0
            poolConfig.setMinIdle(minIdle);
        }
        if (maxIdle > 0) {
            // 最大空闲连接数, 默认8个
            poolConfig.setMaxIdle(maxIdle);
        }
        if (numTestsPerEvictionRun > 0) {
            // 每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n), 默认3
            poolConfig.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
        }
        if (maxTotal != 8) {
            // 最大连接数, 默认8个
            poolConfig.setMaxTotal(maxTotal);
        }
        if (minEvictableIdleTimeMillis > 0) {
            // 逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
            poolConfig.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        }
        if (password != null && password.length() > 0) {
            jedisPool = new JedisPool(poolConfig, host, port, timeout, password);
        } else {
            jedisPool = new JedisPool(poolConfig, host, port, timeout);
        }
        // 配置连接实验
        SimpleDateFormat formater = new SimpleDateFormat(this.getDateFormatter());
        String currentTimeStr = formater.format(new Date());
        try{
            Jedis jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            jedisHeath.set(true);
            System.out.println("[" + currentTimeStr + "] successfully connected to redis[" + this.getHost() + ":"
                    + this.getPort() + "]!");
        } catch(Exception e){
            jedisHeath.set(false);
            System.out.println("[" + currentTimeStr + "] can not connected to redis[" + this.getHost() + ":"
                    + this.getPort() + "]!");
            e.printStackTrace();
        }
    }

    private void closeJedisInstance(Jedis jedis) {
        if (null != jedis) {
            if (jedisPool != null) {
                try {
                    jedisPool.returnBrokenResource(jedis);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            jedis = null;
        }
    }

    public Jedis getWriteJedis_01() {
        if (!jedisHeath.get() || null == _write_jedis_01 || !_write_jedis_01.isConnected()) {
            try {
                _write_jedis_01 = jedisPool.getResource();
                _write_jedis_01.select(getDbIndex());
            }
            catch (Exception e) {
                e.printStackTrace();
                jedisHeath.set(false);
                return null;
            }
        }
        return _write_jedis_01;
    }

    public Jedis getWriteJedis_02() {
        if (!jedisHeath.get() || null == _write_jedis_02 || !_write_jedis_02.isConnected()) {
            try {
                _write_jedis_02 = jedisPool.getResource();
                _write_jedis_02.select(getDbIndex());
            }
            catch (Exception e) {
                e.printStackTrace();
                jedisHeath.set(false);
                return null;
            }
        }
        return _write_jedis_02;
    }

    public void sendLog(String msg) {
        msg = msg.substring(0, msg.length() - nextLineChar.length());
    }

    private void initConfig() throws Exception {
        File configFile = new File(configFilePath);
        Properties properties = new Properties();
        FileInputStream inStream = new FileInputStream(configFile);
        properties.load(inStream);
        host = properties.getProperty("redis_host");
        port = Integer.parseInt(properties.getProperty("redis_port"));
        password = properties.getProperty("redis_pwd");
        dbIndex = Integer.parseInt(properties.getProperty("redis_dbindex"));
    }

    public static ThreadPoolExecutor getExecutor(){
        if(_executor == null || _executor.isShutdown()){
            synchronized (RedisUtils.class){
                //线程池容量
                int threadPoolSize = 500 ;
                _executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadPoolSize, new ThreadFactory() {
                    ThreadGroup tg = new ThreadGroup("pool-redis-executor") ;
                    int count = 0 ;
                    @Override
                    public Thread newThread(Runnable r) {
                        String threadName = "thread-RedisAppender-" + count++ ;
                        return new Thread(tg,r,threadName);
                    }
                });
            }
        }
        return _executor ;
    }

    public static byte[] compress(String str) {
        if (str == null || str.length() == 0) {
            return null;
        }
        ByteArrayOutputStream out = null;
        GZIPOutputStream gzip = null;
        try {
            out = new ByteArrayOutputStream();
            gzip = new GZIPOutputStream(out);
            gzip.write(str.getBytes());
            gzip.finish();
            gzip.close();
            out.close();
            return out.toByteArray();
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        finally {
            try {
                if (gzip != null) {
                    gzip.close();
                }
                if (out != null) {
                    out.close();
                }
            }
            catch (IOException e) {
                // e.printStackTrace();
            }
        }
    }

    public static String encryptHex(byte[] arrB) {
        int iLen = arrB.length;
        StringBuffer sb = new StringBuffer(iLen * 2);
        for (int i = 0; i < iLen; ++i) {
            int intTmp = arrB[i];
            while (intTmp < 0) {
                intTmp += 256;
            }
            if (intTmp < 16) {
                sb.append("0");
            }
            sb.append(Integer.toString(intTmp, 16));
        }
        return sb.toString();
    }

    public static List<RedisStoredInfo> paushXmlConfig(String path) throws DocumentException {
            List<RedisStoredInfo> redisStoredInfos = new ArrayList<>() ;
            SAXReader reader = new SAXReader() ;
            Document doc = reader.read(new File(path)) ;
            Element root = doc.getRootElement() ;
            Iterator<Element> it = root.elementIterator();
            while(it.hasNext()){
                Element serELem = it.next() ;
                Iterator<Element> it2 = serELem.elementIterator() ;
                RedisStoredInfo redisStoredInfo = new RedisStoredInfo() ;
                while(it2.hasNext()){
                    Element service = it2.next() ;
                    if("name".equalsIgnoreCase(service.getName())){
                        redisStoredInfo.setServiceName(service.getText()) ;
                    } else if("id".equalsIgnoreCase(service.getName())){
                        redisStoredInfo.setServiceId(service.getText()) ;
                    } else if("storeType".equalsIgnoreCase(service.getName())){
                        redisStoredInfo.setStoreType(service.getText()) ;
                    } else if("valueList".equalsIgnoreCase(service.getName())){
                        redisStoredInfo.setValueList(Arrays.asList(service.getText().split(","))) ;
                    } else if("expireTime".equalsIgnoreCase(service.getName())){
                        redisStoredInfo.setExpireTime(service.getText());
                    }
                }
                redisStoredInfos.add(redisStoredInfo) ;
            }
            return redisStoredInfos ;
    }

    public static String bytes2HexStr(byte[] arr){
            int iLen = arr.length ;
            StringBuilder sb = new StringBuilder(iLen * 2) ;
            for (int i = 0; i < iLen ; i++) {
                int intTmp = arr[i] ;
                while(intTmp < 0){
                    intTmp += 256 ;
                }
                if(intTmp < 16){
                    sb.append("0") ;
                }
                sb.append(Integer.toString(intTmp,16)) ;
            }
            return sb.toString() ;
    }

}
