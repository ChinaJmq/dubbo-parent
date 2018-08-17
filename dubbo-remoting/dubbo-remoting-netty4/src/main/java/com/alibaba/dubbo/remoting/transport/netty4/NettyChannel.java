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
package com.alibaba.dubbo.remoting.transport.netty4;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.transport.AbstractChannel;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * NettyChannel.
 */
final class NettyChannel extends AbstractChannel {

    private static final Logger logger = LoggerFactory.getLogger(NettyChannel.class);
    /**
     * key 是io.netty.channel.Channel中的channel
     * 通道集合
     */
    private static final ConcurrentMap<Channel, NettyChannel> channelMap = new ConcurrentHashMap<Channel, NettyChannel>();
    /**
     * io.netty.channel.Channel中的channel
     * 通道
     */
    private final Channel channel;
    /**
     * 属性集合
     */
    private final Map<String, Object> attributes = new ConcurrentHashMap<String, Object>();

    /**
     * channel 属性，通道。NettyChannel 是传入 channel 属性的装饰器，每个实现的方法，都会调用 channel 。
     * @param channel
     * @param url
     * @param handler
     */
    private NettyChannel(Channel channel, URL url, ChannelHandler handler) {
        super(url, handler);
        if (channel == null) {
            throw new IllegalArgumentException("netty channel == null;");
        }
        this.channel = channel;
    }

    /**
     * 传递的是 io.netty.channel.Channel 对象。
     * 通过 NettyChannel.channelMap 中，获得对应的 NettyChannel 对象
     *创建 NettyChannel 对象
     * @param ch
     * @param url
     * @param handler
     * @return
     */
    static NettyChannel getOrAddChannel(Channel ch, URL url, ChannelHandler handler) {
        if (ch == null) {
            return null;
        }
        NettyChannel ret = channelMap.get(ch);
        if (ret == null) {
            NettyChannel nettyChannel = new NettyChannel(ch, url, handler);
            if (ch.isActive()) {
                ret = channelMap.putIfAbsent(ch, nettyChannel);
            }
            if (ret == null) {
                ret = nettyChannel;
            }
        }
        return ret;
    }

    /**
     *移除 NettyChannel 对象
     * @param ch
     */
    static void removeChannelIfDisconnected(Channel ch) {
        /**
         * ch.isActive() 通道是否激活
         */
        if (ch != null && !ch.isActive()) {// 未连接
            channelMap.remove(ch);// 移除出channelMap
        }
    }

    /**
     * 返回绑定的地址，服务端的Channel返回监听的地址，而客户端的Channel返回连接到服务端的本地套接字
     * @return
     */
    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
    }
    /**
     * 返回channel的远程套接字。
     * @return
     */
    @Override
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) channel.remoteAddress();
    }

    /**
     * 是否连接
     * @return
     */
    @Override
    public boolean isConnected() {
        return !isClosed() && channel.isActive();
    }

    /**
     * 发送消息
     * @param message
     * @param sent    already sent to socket?
     * @throws RemotingException
     */
    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        //验证channel是否关闭
        super.send(message, sent);

        boolean success = true;
        int timeout = 0;
        try {
            // io.netty.channel.Channel中的channel发送消息
            ChannelFuture future = channel.writeAndFlush(message);
            // 等待发送成功
            if (sent) {
                timeout = getUrl().getPositiveParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT);
                //在Netty中，所有的I/O操作都是异步的，因此调用一个I/O操作后，将继续当前线程的执行
                //在timeout是否发送完成
                success = future.await(timeout);
            }
            //返回结果是否是异常信息
            Throwable cause = future.cause();
            if (cause != null) {
                throw cause;
            }
        } catch (Throwable e) {
            throw new RemotingException(this, "Failed to send message " + message + " to " + getRemoteAddress() + ", cause: " + e.getMessage(), e);
        }
        //未发送成功，跑出异常
        if (!success) {
            throw new RemotingException(this, "Failed to send message " + message + " to " + getRemoteAddress()
                    + "in timeout(" + timeout + "ms) limit");
        }
    }

    /**
     * 关闭通道
     */
    @Override
    public void close() {
        try {
            // 标记关闭
            super.close();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            // 移除NettyChannel
            removeChannelIfDisconnected(channel);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            // 清空属性 attributes
            attributes.clear();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            if (logger.isInfoEnabled()) {
                logger.info("Close netty channel " + channel);
            }
            // 关闭真正的通道 channel
            channel.close();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

    @Override
    public boolean hasAttribute(String key) {
        return attributes.containsKey(key);
    }

    @Override
    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    @Override
    public void setAttribute(String key, Object value) {
        if (value == null) { // The null value unallowed in the ConcurrentHashMap.
            attributes.remove(key);
        } else {
            attributes.put(key, value);
        }
    }

    @Override
    public void removeAttribute(String key) {
        attributes.remove(key);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((channel == null) ? 0 : channel.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        NettyChannel other = (NettyChannel) obj;
        if (channel == null) {
            if (other.channel != null) return false;
        } else if (!channel.equals(other.channel)) return false;
        return true;
    }

    @Override
    public String toString() {
        return "NettyChannel [channel=" + channel + "]";
    }

}