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
package com.alibaba.dubbo.remoting.transport;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.Endpoint;
import com.alibaba.dubbo.remoting.RemotingException;

/**
 * AbstractPeer
 */
public abstract class AbstractPeer implements Endpoint, ChannelHandler {
    /**
     2:  * 通道处理器
     3:  */
    private final ChannelHandler handler;
    /**
     6:  * URL
     7:  */
    private volatile URL url;
    /**
     10:  * 正在关闭
     11:  *
     12:  * {@link #startClose()}
     13:  */
    // closing closed means the process is being closed and close is finished
    private volatile boolean closing;
    /**
     17:  * 关闭完成
     18:  *
     19:  * {@link #close()}
     20:  */
    private volatile boolean closed;

    /**
     *
     * @param url 通过该属性，传递 Dubbo 服务引用和服务暴露的配置项
     * @param handler 通道处理器，通过构造方法传入。实现的 ChannelHandler 的接口方法，直接调用 handler 的方法，进行执行逻辑处理
     *                设计模式中被称作 “装饰模式” 。在下文中，我们会看到大量的装饰模式的使用。实际上，这也是 dubbo-remoting 抽象 API + 实现最核心的方式之一。
     */
    public AbstractPeer(URL url, ChannelHandler handler) {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler == null");
        }
        this.url = url;
        this.handler = handler;
    }

    /**
     * 发送消息
     * sent 配置项
     * true 等待消息发出，消息发送失败将抛出异常
     * false 不等待消息发出，将消息放入 IO 队列，即刻返回。
     * 详细参见：《Dubbo 用户指南 —— 异步调用》http://dubbo.apache.org/books/dubbo-user-book/demos/async-call.html
     * @param message
     * @throws RemotingException
     */
    @Override
    public void send(Object message) throws RemotingException {
        //模板方法，具体有子类实现
        send(message, url.getParameter(Constants.SENT_KEY, false));
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void close(int timeout) {
        close();
    }

    @Override
    public void startClose() {
        if (isClosed()) {
            return;
        }
        closing = true;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    protected void setUrl(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        this.url = url;
    }
    /**
     * 获得通道处理器
     *
     * @return 通道处理器
     */
    @Override
    public ChannelHandler getChannelHandler() {
        if (handler instanceof ChannelHandlerDelegate) {
            // 获得真实的 ChannelHandler
            return ((ChannelHandlerDelegate) handler).getHandler();
        } else {
            return handler;
        }
    }

    /**
     * @return ChannelHandler
     */
    @Deprecated
    public ChannelHandler getHandler() {
        return getDelegateHandler();
    }

    /**
     * Return the final handler (which may have been wrapped). This method should be distinguished with getChannelHandler() method
     *
     * @return ChannelHandler
     */
    public ChannelHandler getDelegateHandler() {
        return handler;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    public boolean isClosing() {
        return closing && !closed;
    }

    @Override
    public void connected(Channel ch) throws RemotingException {
        if (closed) {
            return;
        }
        handler.connected(ch);
    }

    @Override
    public void disconnected(Channel ch) throws RemotingException {
        handler.disconnected(ch);
    }

    @Override
    public void sent(Channel ch, Object msg) throws RemotingException {
        if (closed) {
            return;
        }
        handler.sent(ch, msg);
    }

    @Override
    public void received(Channel ch, Object msg) throws RemotingException {
        if (closed) {
            return;
        }
        handler.received(ch, msg);
    }

    @Override
    public void caught(Channel ch, Throwable ex) throws RemotingException {
        handler.caught(ch, ex);
    }
}
