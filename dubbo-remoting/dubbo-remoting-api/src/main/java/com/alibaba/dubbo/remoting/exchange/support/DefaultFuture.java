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
package com.alibaba.dubbo.remoting.exchange.support;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.TimeoutException;
import com.alibaba.dubbo.remoting.exchange.Request;
import com.alibaba.dubbo.remoting.exchange.Response;
import com.alibaba.dubbo.remoting.exchange.ResponseCallback;
import com.alibaba.dubbo.remoting.exchange.ResponseFuture;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * DefaultFuture.
 * 实现 ResponseFuture 接口，默认响应 Future 实现类。
 * 实现异步的请求-响应
 * 同时，它也是所有 DefaultFuture 的管理容器。
 */
public class DefaultFuture implements ResponseFuture {

    private static final Logger logger = LoggerFactory.getLogger(DefaultFuture.class);
    /**
     * 通道集合
     * key：请求编号
     * key=Request的ID，value=Channel
     */
    private static final Map<Long, Channel> CHANNELS = new ConcurrentHashMap<Long, Channel>();
    /**
     * Future 集合
     * key：请求编号
     * key=Request的ID，value=DefaultFuture
     */
    private static final Map<Long, DefaultFuture> FUTURES = new ConcurrentHashMap<Long, DefaultFuture>();
    //初始化后台扫描调用超时任务的守护线程
    static {
        Thread th = new Thread(new RemotingInvocationTimeoutScan(), "DubboResponseTimeoutScanTimer");
        //设置为守护线程
        th.setDaemon(true);
        th.start();
    }

    /**
     * 请求编号
     */
    // invoke id.
    private final long id;
    /**
     * 通道
     */
    private final Channel channel;
    /**
     * 请求
     */
    private final Request request;
    /**
     * 超时
     */
    private final int timeout;
    private final Lock lock = new ReentrantLock();
    private final Condition done = lock.newCondition();
    /**
     * 创建开始时间
     */
    private final long start = System.currentTimeMillis();
    /**
     * 发送请求时间
     */
    private volatile long sent;
    /**
     * 响应
     */
    private volatile Response response;
    /**
     * 回调
     */
    private volatile ResponseCallback callback;

    public DefaultFuture(Channel channel, Request request, int timeout) {
        this.channel = channel;
        this.request = request;
        this.id = request.getId();
        this.timeout = timeout > 0 ? timeout : channel.getUrl().getPositiveParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT);
        // put into waiting map.
        FUTURES.put(id, this);
        CHANNELS.put(id, channel);
    }

    /**
     * 根据id获取对应的DefaultFuture
     * @param id
     * @return
     */
    public static DefaultFuture getFuture(long id) {
        return FUTURES.get(id);
    }

    /**
     * CHANNELS 静态属性，通道集合。通过 #hasFuture(channel) 方法，判断通道是否有未结束的请求
     * @param channel
     * @return
     */
    public static boolean hasFuture(Channel channel) {
        return CHANNELS.containsValue(channel);
    }

    /**
     * 设置 sent 属性，发送请求时间
     * 因为在目前 Netty Mina 等通信框架中，发送请求一般是异步的，
     * 因此在 ChannelHandler#sent(channel, message) 方法中，调用 DefaultFuture#sent(channel, request) 静态方法
     * @param channel
     * @param request
     */
    public static void sent(Channel channel, Request request) {
        DefaultFuture future = FUTURES.get(request.getId());
        if (future != null) {
            future.doSent();
        }
    }

    /**
     * 响应结果
     * @param channel
     * @param response
     */
    public static void received(Channel channel, Response response) {
        try {
            // 移除 FUTURES
            DefaultFuture future = FUTURES.remove(response.getId());
            // 接收结果
            if (future != null) {
                future.doReceived(response);
            } else {
                logger.warn("The timeout response finally returned at "
                        + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()))
                        + ", response " + response
                        + (channel == null ? "" : ", channel: " + channel.getLocalAddress()
                        + " -> " + channel.getRemoteAddress()));
            }
        } finally {
            // 移除 CHANNELS
            CHANNELS.remove(response.getId());
        }
    }

    @Override
    public Object get() throws RemotingException {
        return get(timeout);
    }

    /**
     * 获取结果
     * @param timeout
     * @return
     * @throws RemotingException
     */
    @Override
    public Object get(int timeout) throws RemotingException {
        if (timeout <= 0) {
            timeout = Constants.DEFAULT_TIMEOUT;
        }
        // 若未完成，等待
        if (!isDone()) {
            /**
             * 获得开始时间。注意，此处使用的不是 start 属性。后面我们会看到，#get(...) 方法中，使用的是重新获取开始时间；
             * 后台扫描调用超时任务，使用的是 start 属性。也就是说，#get(timeout) 方法的 timeout 参数，指的是从当前时刻开始的等待超时时间。
             * 当然，这不影响最终的结果，最终 Response 是什么，由是 ChannelHandler#received(channel, message) 还是后台扫描调用超时任务，
             * 谁先调用 DefaultFuture#received(channel, response) 方法决定
             */
            long start = System.currentTimeMillis();
            lock.lock();
            try {
                // 等待完成或超时
                while (!isDone()) {
                    done.await(timeout, TimeUnit.MILLISECONDS);
                    if (isDone() || System.currentTimeMillis() - start > timeout) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
            // 超时时间内未完成，抛出超时异常 TimeoutException
            if (!isDone()) {
                throw new TimeoutException(sent > 0, channel, getTimeoutMessage(false));
            }
        }
        // 返回响应
        return returnFromResponse();
    }

    /**
     * 1.根据对应的id构建响应信息，并赋值给response属性
     * 2.根据id移除DefaultFuture
     * 3.根据id移除Channel
     */
    public void cancel() {
        Response errorResult = new Response(id);
        errorResult.setErrorMessage("request future has been canceled.");
        response = errorResult;
        FUTURES.remove(id);
        CHANNELS.remove(id);
    }

    /**
     * 通过response来判断是否已经返回响应
     * @return
     */
    @Override
    public boolean isDone() {
        return response != null;
    }

    /**
     * 设置回调
     * @param callback
     */
    @Override
    public void setCallback(ResponseCallback callback) {
        // 已完成，调用回调
        if (isDone()) {
            invokeCallback(callback);
        } else {
            boolean isdone = false;
            lock.lock();
            try {
                // 未完成，设置回调,等在 #doReceived(response) 方法中再回调
                if (!isDone()) {
                    this.callback = callback;
                } else {
                    isdone = true;
                }
            } finally {
                lock.unlock();
            }
            // 已完成，调用回调
            if (isdone) {
                invokeCallback(callback);
            }
        }
    }

    /**
     * 执行回调
     * @param c
     */
    private void invokeCallback(ResponseCallback c) {
        ResponseCallback callbackCopy = c;
        if (callbackCopy == null) {
            throw new NullPointerException("callback cannot be null.");
        }
        c = null;
        Response res = response;
        if (res == null) {
            throw new IllegalStateException("response cannot be null. url:" + channel.getUrl());
        }
        // 正常，处理结果
        if (res.getStatus() == Response.OK) {
            try {
                callbackCopy.done(res.getResult());
            } catch (Exception e) {
                logger.error("callback invoke error .reasult:" + res.getResult() + ",url:" + channel.getUrl(), e);
            }
            // 超时，处理 TimeoutException 异常
        } else if (res.getStatus() == Response.CLIENT_TIMEOUT || res.getStatus() == Response.SERVER_TIMEOUT) {
            try {
                TimeoutException te = new TimeoutException(res.getStatus() == Response.SERVER_TIMEOUT, channel, res.getErrorMessage());
                callbackCopy.caught(te);
            } catch (Exception e) {
                logger.error("callback invoke error ,url:" + channel.getUrl(), e);
            }
        } else {
            // 其他，处理 RemotingException 异常
            try {
                RuntimeException re = new RuntimeException(res.getErrorMessage());
                callbackCopy.caught(re);
            } catch (Exception e) {
                logger.error("callback invoke error ,url:" + channel.getUrl(), e);
            }
        }
    }

    /**
     * 返回响应( Response )
     * @return
     * @throws RemotingException
     */
    private Object returnFromResponse() throws RemotingException {
        Response res = response;
        if (res == null) {
            throw new IllegalStateException("response cannot be null");
        }
        // 正常，返回结果
        if (res.getStatus() == Response.OK) {
            return res.getResult();
        }
        // 超时，抛出 TimeoutException 异常
        if (res.getStatus() == Response.CLIENT_TIMEOUT || res.getStatus() == Response.SERVER_TIMEOUT) {
            throw new TimeoutException(res.getStatus() == Response.SERVER_TIMEOUT, channel, res.getErrorMessage());
        }
        // 其他，抛出 RemotingException 异常
        throw new RemotingException(channel, res.getErrorMessage());
    }

    private long getId() {
        return id;
    }

    private Channel getChannel() {
        return channel;
    }

    private boolean isSent() {
        return sent > 0;
    }

    public Request getRequest() {
        return request;
    }

    private int getTimeout() {
        return timeout;
    }

    private long getStartTimestamp() {
        return start;
    }

    private void doSent() {
        sent = System.currentTimeMillis();
    }

    /**
     *
     * @param res
     */
    private void doReceived(Response res) {
        //获得锁
        lock.lock();
        try {
            // 设置结果
            response = res;
            // 通知，唤醒efaultFuture#get(..) 方法的等待
            if (done != null) {
                done.signal();
            }
        } finally {
            //释放锁
            lock.unlock();
        }
        //执行回调方法
        if (callback != null) {
            invokeCallback(callback);
        }
    }

    /**
     * 设置超时异常信息
     * @param scan 是否是定时任务扫描
     * @return
     */
    private String getTimeoutMessage(boolean scan) {
        long nowTimestamp = System.currentTimeMillis();
        return (sent > 0 ? "Waiting server-side response timeout" : "Sending request timeout in client-side")
                + (scan ? " by scan timer" : "") + ". start time: "
                + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(start))) + ", end time: "
                + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())) + ","
                + (sent > 0 ? " client elapsed: " + (sent - start)
                + " ms, server elapsed: " + (nowTimestamp - sent)
                : " elapsed: " + (nowTimestamp - start)) + " ms, timeout: "
                + timeout + " ms, request: " + request + ", channel: " + channel.getLocalAddress()
                + " -> " + channel.getRemoteAddress();
    }

    private static class RemotingInvocationTimeoutScan implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    for (DefaultFuture future : FUTURES.values()) {
                        // 已完成，跳过
                        if (future == null || future.isDone()) {
                            continue;
                        }
                        // 超时
                        if (System.currentTimeMillis() - future.getStartTimestamp() > future.getTimeout()) {
                            // create exception response.
                            // 创建超时 Response
                            Response timeoutResponse = new Response(future.getId());
                            // set timeout status.
                            timeoutResponse.setStatus(future.isSent() ? Response.SERVER_TIMEOUT : Response.CLIENT_TIMEOUT);
                            timeoutResponse.setErrorMessage(future.getTimeoutMessage(true));
                            // handle response.
                            // 响应结果
                            DefaultFuture.received(future.getChannel(), timeoutResponse);
                        }
                    }
                    Thread.sleep(30);
                } catch (Throwable e) {
                    logger.error("Exception when scan the timeout invocation of remoting.", e);
                }
            }
        }
    }

}
