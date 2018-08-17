/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.registry.redis;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.NamedThreadFactory;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.support.FailbackRegistry;
import com.alibaba.dubbo.rpc.RpcException;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RedisRegistry
 *
 */
public class RedisRegistry extends FailbackRegistry {

    private static final Logger logger = LoggerFactory.getLogger(RedisRegistry.class);
    /**
     2:  * 默认端口
     3:  */
    private static final int DEFAULT_REDIS_PORT = 6379;
    /**
     6:  * 默认 Redis 根节点
     7:  */
    private final static String DEFAULT_ROOT = "dubbo";
    /**
     11:  * Redis Key 过期机制执行器
     12:  */
    private final ScheduledExecutorService expireExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("DubboRegistryExpireTimer", true));
    /**
     15:  * Redis Key 过期机制 Future
     16:  */
    private final ScheduledFuture<?> expireFuture;
    /**
     20:  * Redis 根节点
     21:  */
    private final String root;
    /**
     25:  * JedisPool 集合
     26:  *
     27:  * key：ip:port
     28:  */
    private final Map<String, JedisPool> jedisPools = new ConcurrentHashMap<String, JedisPool>();
    /**
     32:  * 通知器集合
     33:  *
     34:  * key：Root + Service ，例如 `/dubbo/com.alibaba.dubbo.demo.DemoService`
     35:  */
    private final ConcurrentMap<String, Notifier> notifiers = new ConcurrentHashMap<String, Notifier>();
    /**
     * 重连周期，单位：毫秒
     */
    private final int reconnectPeriod;

    /**
     * Redis Key 过期周期，单位：毫秒
     */
    private final int expirePeriod;
    /**
     * 是否监控中心
     */
    private volatile boolean admin = false;
    /**
     * 是否复制模式
     */
    private boolean replicate;

    public RedisRegistry(URL url) {
        super(url);
        if (url.isAnyHost()) {
            throw new IllegalStateException("registry address == null");
        }
        //参考文档http://fanyi.youdao.com/WebpageTranslate?url=http://commons.apache.org/proper/commons-pool/api-1.6/org/apache/commons/pool/impl/GenericObjectPool.html&type=AUTO&action=%C2%A1%C2%A4-%C2%A8%C2%B0?&keyfrom=360ce
        // 创建 GenericObjectPoolConfig 对象
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        //在borrow(引入)一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
        config.setTestOnBorrow(url.getParameter("test.on.borrow", true));
        //return 一个jedis实例给pool时，是否检查连接可用性（ping()）
        config.setTestOnReturn(url.getParameter("test.on.return", false));
        //如果为true，表示有一个idle object evitor线程对idle object进行扫描，如果validate失败，
        // 此object会被从pool中drop掉；这一项只有在timeBetweenEvictionRunsMillis大于0时才有意义；
        config.setTestWhileIdle(url.getParameter("test.while.idle", false));
        //控制一个pool最多有多少个状态为idle的jedis实例；
        if (url.getParameter("max.idle", 0) > 0)
            config.setMaxIdle(url.getParameter("max.idle", 0));
        if (url.getParameter("min.idle", 0) > 0)
            config.setMinIdle(url.getParameter("min.idle", 0));
        if (url.getParameter("max.active", 0) > 0)
            config.setMaxTotal(url.getParameter("max.active", 0));
        //控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
        //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
        if (url.getParameter("max.total", 0) > 0)
            config.setMaxTotal(url.getParameter("max.total", 0));
        //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；单位毫秒
        //小于零:阻塞不确定的时间,  默认-1
        if (url.getParameter("max.wait", url.getParameter("timeout", 0)) > 0)
            config.setMaxWaitMillis(url.getParameter("max.wait", url.getParameter("timeout", 0)));
        //表示idle object evitor每次扫描的最多的对象数；
        if (url.getParameter("num.tests.per.eviction.run", 0) > 0)
            config.setNumTestsPerEvictionRun(url.getParameter("num.tests.per.eviction.run", 0));
        //表示idle object evitor两次扫描之间要sleep的毫秒数；
        if (url.getParameter("time.between.eviction.runs.millis", 0) > 0)
            config.setTimeBetweenEvictionRunsMillis(url.getParameter("time.between.eviction.runs.millis", 0));
        //表示一个对象至少停留在idle状态的最短时间，然后才能被idle object evitor扫描并驱逐；这一项只有在timeBetweenEvictionRunsMillis大于0时才有意义；
        if (url.getParameter("min.evictable.idle.time.millis", 0) > 0)
            config.setMinEvictableIdleTimeMillis(url.getParameter("min.evictable.idle.time.millis", 0));

        // 是否复制模式
        String cluster = url.getParameter("cluster", "failover");
        if (!"failover".equals(cluster) && !"replicate".equals(cluster)) {
            throw new IllegalArgumentException("Unsupported redis cluster: " + cluster + ". The redis cluster only supported failover or replicate.");
        }
        replicate = "replicate".equals(cluster);

        // 解析redis集群addresses
        List<String> addresses = new ArrayList<String>();
        addresses.add(url.getAddress());
        String[] backups = url.getParameter(Constants.BACKUP_KEY, new String[0]);
        if (backups != null && backups.length > 0) {
            addresses.addAll(Arrays.asList(backups));
        }

        // 创建 JedisPool 对象
        String password = url.getPassword();
        for (String address : addresses) {
            int i = address.indexOf(':');
            String host;
            int port;
            if (i > 0) {
                host = address.substring(0, i);
                port = Integer.parseInt(address.substring(i + 1));
            } else {
                host = address;
                port = DEFAULT_REDIS_PORT;
            }
            if (StringUtils.isEmpty(password)) {
                this.jedisPools.put(address, new JedisPool(config, host, port,
                        url.getParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT)));
            } else {
                this.jedisPools.put(address, new JedisPool(config, host, port,
                        url.getParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT), password));
            }
        }

        // 解析重连周期
        this.reconnectPeriod = url.getParameter(Constants.REGISTRY_RECONNECT_PERIOD_KEY, Constants.DEFAULT_REGISTRY_RECONNECT_PERIOD);
        // 获得 Redis 根节点
        String group = url.getParameter(Constants.GROUP_KEY, DEFAULT_ROOT);
        if (!group.startsWith(Constants.PATH_SEPARATOR)) {
            group = Constants.PATH_SEPARATOR + group;
        }
        if (!group.endsWith(Constants.PATH_SEPARATOR)) {
            group = group + Constants.PATH_SEPARATOR;
        }
        this.root = group;

        // 创建实现 Redis Key 过期机制的任务
        this.expirePeriod = url.getParameter(Constants.SESSION_TIMEOUT_KEY, Constants.DEFAULT_SESSION_TIMEOUT);
        this.expireFuture = expireExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    deferExpired(); // Extend the expiration time
                } catch (Throwable t) { // Defensive fault tolerance
                    logger.error("Unexpected exception occur at defer expire time, cause: " + t.getMessage(), t);
                }
            }
        }, expirePeriod / 2, expirePeriod / 2, TimeUnit.MILLISECONDS);
    }

    /**
     * 推迟过期时间
     */
    private void deferExpired() {
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                    // 循环已注册的 URL 集合
                    for (URL url : new HashSet<URL>(getRegistered())) {
                        // 判断是否为动态节点，只有动态节点需要延长过期时间
                        if (url.getParameter(Constants.DYNAMIC_KEY, true)) {
                            // 获得分类路径
                            String key = toCategoryPath(url);
                            //返回值:1.如果 field 是哈希表中的一个新建域，并且值设置成功，返回1 。
                            //2.如果哈希表中域 field 已经存在且旧值已被新值覆盖，返回0 。
                            if (jedis.hset(key, url.toFullString(), String.valueOf(System.currentTimeMillis() + expirePeriod)) == 1) {
                                // 发布 `register` 事件。
                                jedis.publish(key, Constants.REGISTER);
                            }
                        }
                    }
                    // 监控中心负责删除过期脏数据
                    if (admin) {
                        clean(jedis);
                    }
                    // 如果服务器端已同步数据，只需写入单台机器
                    if (!replicate) {
                        break;//  If the server side has synchronized data, just write a single machine
                    }
                } finally {
                    jedis.close();
                }
            } catch (Throwable t) {
                logger.warn("Failed to write provider heartbeat to redis registry. registry: " + entry.getKey() + ", cause: " + t.getMessage(), t);
            }
        }
    }



    /**
     * 监控中心负责删除过去的脏数据
     * @param jedis
     */
    // The monitoring center is responsible for deleting outdated dirty data
    private void clean(Jedis jedis) {
        // 获得所有服务
        Set<String> keys = jedis.keys(root + Constants.ANY_VALUE);
        if (keys != null && !keys.isEmpty()) {
            for (String key : keys) {
                Map<String, String> values = jedis.hgetAll(key);
                if (values != null && values.size() > 0) {
                    boolean delete = false;
                    long now = System.currentTimeMillis();
                    for (Map.Entry<String, String> entry : values.entrySet()) {
                        URL url = URL.valueOf(entry.getKey());
                        // 动态节点
                        if (url.getParameter(Constants.DYNAMIC_KEY, true)) {
                            long expire = Long.parseLong(entry.getValue());
                            // 已经过期
                            if (expire < now) {
                                jedis.hdel(key, entry.getKey());
                                delete = true;
                                if (logger.isWarnEnabled()) {
                                    logger.warn("Delete expired key: " + key + " -> value: " + entry.getKey() + ", expire: " + new Date(expire) + ", now: " + new Date(now));
                                }
                            }
                        }
                    }
                    if (delete) {
                        jedis.publish(key, Constants.UNREGISTER);
                    }
                }
            }
        }
    }

    @Override
    public boolean isAvailable() {
        for (JedisPool jedisPool : jedisPools.values()) {
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                    if (jedis.isConnected()) {// 至少一个 Redis 节点可用
                        return true; // At least one single machine is available.
                    }
                } finally {
                    jedis.close();
                }
            } catch (Throwable t) {
            }
        }
        return false;
    }

    @Override
    public void destroy() {
        // 父类关闭
        super.destroy();
        // 关闭定时任务
        try {
            expireFuture.cancel(true);
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        // 关闭通知器
        try {
            for (Notifier notifier : notifiers.values()) {
                notifier.shutdown();
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        // 关闭连接池
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                jedisPool.destroy();
            } catch (Throwable t) {
                logger.warn("Failed to destroy the redis registry client. registry: " + entry.getKey() + ", cause: " + t.getMessage(), t);
            }
        }
    }

    /**
     * 注册
     * @param url
     */
    @Override
    public void doRegister(URL url) {
        //获得分类路径作为 Key
        String key = toCategoryPath(url);
        //获得 URL 字符串作为 Value
        String value = url.toFullString();
        // 计算过期时间
        String expire = String.valueOf(System.currentTimeMillis() + expirePeriod);
        boolean success = false;
        RpcException exception = null;
        // 向 Redis 注册
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                    //过期时间，作为 Map 的值。
                    jedis.hset(key, value, expire);
                    //发布 register 事件。这样订阅该 Key 的服务消费者和监控中心，就会实时从 Redis 读取该服务的最新数据
                    jedis.publish(key, Constants.REGISTER);
                    success = true;
                    //failover: 只写入和读取任意一台，失败时重试另一台，需要服务器端自行配置数据同步
                    //replicate: 在客户端同时写入所有服务器，只读取单台，服务器端不需要同步，注册中心集群增大，性能压力也会更大
                    if (!replicate) {
                        break; //  If the server side has synchronized data, just write a single machine
                    }
                } finally {
                    jedis.close();
                }
            } catch (Throwable t) {
                exception = new RpcException("Failed to register service to redis registry. registry: " + entry.getKey() + ", service: " + url + ", cause: " + t.getMessage(), t);
            }
        }
        if (exception != null) {
            if (success) {
                logger.warn(exception.getMessage(), exception);
            } else {
                throw exception;
            }
        }
    }

    /**
     * 取消注册
     * @param url
     */
    @Override
    public void doUnregister(URL url) {
        String key = toCategoryPath(url);
        String value = url.toFullString();
        RpcException exception = null;
        boolean success = false;
        // 向 Redis 取消注册
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                    // 删除 Redis Map 键
                    jedis.hdel(key, value);
                    // 发布 Redis 取消注册事件
                    jedis.publish(key, Constants.UNREGISTER);
                    success = true;
                    //  如果服务器端已同步数据，只需写入单台机器
                    if (!replicate) {
                        break; //  If the server side has synchronized data, just write a single machine
                    }
                } finally {
                    jedis.close();
                }
            } catch (Throwable t) {
                exception = new RpcException("Failed to unregister service to redis registry. registry: " + entry.getKey() + ", service: " + url + ", cause: " + t.getMessage(), t);
            }
        }
        // 处理异常
        if (exception != null) {
            // 虽然发生异常，但是结果成功
            if (success) {
                logger.warn(exception.getMessage(), exception);
                // 最终未成功
            } else {
                throw exception;
            }
        }
    }

    /**
     * 订阅
     * @param url
     * @param listener
     */
    @Override
    public void doSubscribe(final URL url, final NotifyListener listener) {
        // 获得服务路径，例如：`/dubbo/com.alibaba.dubbo.demo.DemoService`
        String service = toServicePath(url);
        // 获得通知器 Notifier 对象
        Notifier notifier = notifiers.get(service);
        if (notifier == null) {
            Notifier newNotifier = new Notifier(service);
            notifiers.putIfAbsent(service, newNotifier);
            notifier = notifiers.get(service);
            if (notifier == newNotifier) { // 保证并发的情况下，有且仅有一个启动
                notifier.start();
            }
        }
        boolean success = false;
        RpcException exception = null;
        // 循环 `jedisPools` ，仅向一个 Redis 发起订阅
        for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            try {
                Jedis jedis = jedisPool.getResource();
                try {
                    // 处理所有 Service 层的发起订阅，例如监控中心的订阅如/dubbo/*
                    if (service.endsWith(Constants.ANY_VALUE)) {
                        //标记 admin = true 。因为，只有注册中心，才清理脏数据
                        admin = true;
                        //获得所有 Service 层下的所有 URL 们。例如 /dubbo/*
                        Set<String> keys = jedis.keys(service);
                        if (keys != null && !keys.isEmpty()) {
                            // 按照服务聚合 URL 集合
                            Map<String, Set<String>> serviceKeys = new HashMap<String, Set<String>>();
                            for (String key : keys) {
                                // 获得服务路径com.alibaba.dubbo.demo.DemoService
                                String serviceKey = toServicePath(key);
                                Set<String> sk = serviceKeys.get(serviceKey);
                                if (sk == null) {
                                    sk = new HashSet<String>();
                                    serviceKeys.put(serviceKey, sk);
                                }
                                sk.add(key);
                            }
                            // 循环 serviceKeys ，按照每个 Service 层的发起通知
                            for (Set<String> sk : serviceKeys.values()) {
                                doNotify(jedis, sk, url, Arrays.asList(listener));
                            }
                        }
                    } else {
                        // 处理指定 Service 层的发起通知，dubbo/com.alibaba.dubbo.demo.DemoService/*
                        doNotify(jedis, jedis.keys(service + Constants.PATH_SEPARATOR + Constants.ANY_VALUE), url, Arrays.asList(listener));
                    }
                    // 标记成功
                    // 结束，仅仅从一台服务器读取数据
                    success = true;
                    break; // Just read one server's data
                } finally {
                    jedis.close();
                }
            } catch (Throwable t) { // Try the next server
                exception = new RpcException("Failed to subscribe service from redis registry. registry: " + entry.getKey() + ", service: " + url + ", cause: " + t.getMessage(), t);
            }
        }
        // 处理异常
        if (exception != null) {
            if (success) {// 虽然发生异常，但是结果成功
                logger.warn(exception.getMessage(), exception);
            } else {// 最终未成功
                throw exception;
            }
        }
    }

    @Override
    public void doUnsubscribe(URL url, NotifyListener listener) {
    }

    private void doNotify(Jedis jedis, String key) {
        for (Map.Entry<URL, Set<NotifyListener>> entry : new HashMap<URL, Set<NotifyListener>>(getSubscribed()).entrySet()) {
            doNotify(jedis, Arrays.asList(key), entry.getKey(), new HashSet<NotifyListener>(entry.getValue()));
        }
    }

    /**
     * 通知
     * @param jedis
     * @param keys  分类数组，元素例如：`/dubbo/com.alibaba.dubbo.demo.DemoService/providers`
     * @param url
     * @param listeners
     */
    private void doNotify(Jedis jedis, Collection<String> keys, URL url, Collection<NotifyListener> listeners) {
        if (keys == null || keys.isEmpty()
                || listeners == null || listeners.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        List<URL> result = new ArrayList<URL>();
        //服务分组
        List<String> categories = Arrays.asList(url.getParameter(Constants.CATEGORY_KEY, new String[0]));
        //消费的服务
        String consumerService = url.getServiceInterface();
        // 循环分类层，例如：`/dubbo/com.alibaba.dubbo.demo.DemoService/providers`
        for (String key : keys) {
            // 若服务不匹配，返回
            if (!Constants.ANY_VALUE.equals(consumerService)) {
                String prvoiderService = toServiceName(key);
                if (!prvoiderService.equals(consumerService)) {
                    continue;
                }
            }
            //分组名
            String category = toCategoryName(key);
            // 若订阅的不包含该分类，返回
            if (!categories.contains(Constants.ANY_VALUE) && !categories.contains(category)) {
                continue;
            }
            // 获得所有 URL 数组
            List<URL> urls = new ArrayList<URL>();
            //获取key下的所有map
            Map<String, String> values = jedis.hgetAll(key);
            //遍历所有的hmap
            if (values != null && values.size() > 0) {
                for (Map.Entry<String, String> entry : values.entrySet()) {
                    //将key转换为url
                    URL u = URL.valueOf(entry.getKey());

                    if (!u.getParameter(Constants.DYNAMIC_KEY, true) // 非动态节点，因为动态节点，不受过期的限制
                            || Long.parseLong(entry.getValue()) >= now) {// 未过期
                        //匹配消费者和提供者
                        if (UrlUtils.isMatch(url, u)) {
                            urls.add(u);
                        }
                    }
                }
            }
            // 若不存在匹配，则创建 `empty://` 的 URL返回，用于清空该服务的该分类。
            if (urls.isEmpty()) {
                urls.add(url.setProtocol(Constants.EMPTY_PROTOCOL)
                        .setAddress(Constants.ANYHOST_VALUE)
                        .setPath(toServiceName(key))
                        .addParameter(Constants.CATEGORY_KEY, category));
            }
            result.addAll(urls);
            if (logger.isInfoEnabled()) {
                logger.info("redis notify: " + key + " = " + urls);
            }
        }
        if (result == null || result.isEmpty()) {
            return;
        }
        // 全量数据获取完成时，调用 `super#notify(...)` 方法，回调 NotifyListener
        for (NotifyListener listener : listeners) {
            notify(url, listener, result);
        }
    }

    /**
     * 获取服务的名字
     * @param categoryPath
     * @return
     */
    private String toServiceName(String categoryPath) {
        String servicePath = toServicePath(categoryPath);
        return servicePath.startsWith(root) ? servicePath.substring(root.length()) : servicePath;
    }

    /**
     * 分组名
     * @param categoryPath
     * @return
     */
    private String toCategoryName(String categoryPath) {
        int i = categoryPath.lastIndexOf(Constants.PATH_SEPARATOR);
        return i > 0 ? categoryPath.substring(i + 1) : categoryPath;
    }
    /**
     * 获得服务路径，主要截掉多余的部分
     *
     * type
     *
     * @param categoryPath 分类路径
     * @return 服务路径
     */
    private String toServicePath(String categoryPath) {
        int i;
        if (categoryPath.startsWith(root)) {
            i = categoryPath.indexOf(Constants.PATH_SEPARATOR, root.length());
        } else {
            i = categoryPath.indexOf(Constants.PATH_SEPARATOR);
        }
        return i > 0 ? categoryPath.substring(0, i) : categoryPath;
    }
    /**
     * 获得服务路径
     *
     * Root + Type
     *
     * @param url URL
     * @return 服务路径
     */
    private String toServicePath(URL url) {
        return root + url.getServiceInterface();
    }
    /**
     * 获得分类路径数组
     *
     * Root + Service + Type
     *
     * @param url URL
     * @return 分类路径数组
     */
    private String toCategoryPath(URL url) {
        return toServicePath(url) + Constants.PATH_SEPARATOR + url.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY);
    }

    /**
     * 实现 redis.clients.jedis.JedisPubSub 抽象类，通知订阅实现类。
     */
    private class NotifySub extends JedisPubSub {

        private final JedisPool jedisPool;

        public NotifySub(JedisPool jedisPool) {
            this.jedisPool = jedisPool;
        }

        @Override
        public void onMessage(String key, String msg) {
            if (logger.isInfoEnabled()) {
                logger.info("redis event: " + key + " = " + msg);
            }
            if (msg.equals(Constants.REGISTER)
                    || msg.equals(Constants.UNREGISTER)) {
                try {
                    Jedis jedis = jedisPool.getResource();
                    try {
                        doNotify(jedis, key);
                    } finally {
                        jedis.close();
                    }
                } catch (Throwable t) { // TODO Notification failure does not restore mechanism guarantee
                    logger.error(t.getMessage(), t);
                }
            }
        }

        /**
         * 订阅得到信息在lister的onPMessage(...)方法中进行处理
         * @param pattern
         * @param key 通道
         * @param msg 消息
         */
        @Override
        public void onPMessage(String pattern, String key, String msg) {
            onMessage(key, msg);
        }

        @Override
        public void onSubscribe(String key, int num) {
        }

        @Override
        public void onPSubscribe(String pattern, int num) {
        }

        @Override
        public void onUnsubscribe(String key, int num) {
        }

        @Override
        public void onPUnsubscribe(String pattern, int num) {
        }

    }

    /**
     * 继承 Thread 类，负责向 Redis 发起订阅逻辑
     */
    private class Notifier extends Thread {
        /**
         2:  * 服务名 Root + Service
         3:  */
        private final String service;
        /**
         22:  * 需要忽略连接的次数
         23:  */
        private final AtomicInteger connectSkip = new AtomicInteger();
        /**
         26:  * 已经忽略连接的次数
         27:  */
        private final AtomicInteger connectSkiped = new AtomicInteger();
        /**
         30:  * 随机
         31:  */
        private final Random random = new Random();
        /**
         6:  * Jedis
         7:  */
        private volatile Jedis jedis;
        /**
         10:  * 是否首次
         11:  */
        private volatile boolean first = true;
        /**
         14:  * 是否运行中
         15:  */
        private volatile boolean running = true;
        /**
         18:  * 连接次数随机数
         19:  */
        private volatile int connectRandom;

        public Notifier(String service) {
            super.setDaemon(true);
            super.setName("DubboRedisSubscribe");
            this.service = service;
        }

        private void resetSkip() {
            // 重置需要连接的次数
            connectSkip.set(0);
            // 重置已忽略次数和随机数
            connectSkiped.set(0);
            connectRandom = 0;
        }

        /**
         * 判断是否忽略本次对 Redis 的连接
         * @return
         */
        private boolean isSkip() {
            // 获得需要忽略连接的总次数。如果超过 10 ，则加上一个 10 以内的随机数。
            int skip = connectSkip.get(); // Growth of skipping times
            if (skip >= 10) { // If the number of skipping times increases by more than 10, take the random number
                if (connectRandom == 0) {
                    connectRandom = random.nextInt(10);
                }
                skip = 10 + connectRandom;
            }
            // 自增忽略次数。若忽略次数不够，则继续忽略。
            if (connectSkiped.getAndIncrement() < skip) { // Check the number of skipping times
                return true;
            }
            // 增加需要忽略的次数
            connectSkip.incrementAndGet();
            // 重置已忽略次数和随机数
            connectSkiped.set(0);
            connectRandom = 0;
            return false;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    // 是否跳过本次 Redis 连接
                    if (!isSkip()) {
                        try {
                            for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
                                JedisPool jedisPool = entry.getValue();
                                try {
                                    jedis = jedisPool.getResource();
                                    try {
                                        // 监控中心
                                        if (service.endsWith(Constants.ANY_VALUE)) {
                                            if (!first) {
                                                first = false;
                                                Set<String> keys = jedis.keys(service);
                                                if (keys != null && !keys.isEmpty()) {
                                                    for (String s : keys) {
                                                        doNotify(jedis, s);
                                                    }
                                                }
                                                resetSkip();
                                            }
                                            // 批订阅,这里启动了订阅监听，线程将在这里被阻塞
                                            jedis.psubscribe(new NotifySub(jedisPool), service); // blocking
                                            // 服务提供者或消费者
                                        } else {
                                            if (!first) {
                                                first = false;
                                                doNotify(jedis, service);
                                                resetSkip();
                                            }
                                            // 批订阅,这里启动了订阅监听，线程将在这里被阻塞
                                            jedis.psubscribe(new NotifySub(jedisPool), service + Constants.PATH_SEPARATOR + Constants.ANY_VALUE); // blocking
                                        }
                                        break;
                                    } finally {
                                        jedis.close();
                                    }
                                } catch (Throwable t) { // Retry another server
                                    logger.warn("Failed to subscribe service from redis registry. registry: " + entry.getKey() + ", cause: " + t.getMessage(), t);
                                    // If you only have a single redis, you need to take a rest to avoid overtaking a lot of CPU resources
                                    //如果只有一台redis,需要休息会，避免占用大量的cpu资源
                                    //用于订阅发生 Redis 连接异常时，Notifier sleep ，等待重连上
                                    sleep(reconnectPeriod);
                                }
                            }
                        } catch (Throwable t) {
                            logger.error(t.getMessage(), t);
                            sleep(reconnectPeriod);
                        }
                    }
                } catch (Throwable t) {
                    logger.error(t.getMessage(), t);
                }
            }
        }

        public void shutdown() {
            try {
                running = false;
                jedis.disconnect();
            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }
        }

    }

}
