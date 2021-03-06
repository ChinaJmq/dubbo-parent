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

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.remoting.Codec2;
import com.alibaba.dubbo.remoting.buffer.ChannelBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;
import java.util.List;

/**
 * NettyCodecAdapter.
 */
final class NettyCodecAdapter {
    /**
     * Netty 编码处理器
     */
    private final ChannelHandler encoder = new InternalEncoder();
    /**
     * Netty 解码处理器
     */
    private final ChannelHandler decoder = new InternalDecoder();
    /**
     * Dubbo 编解码器
     */
    private final Codec2 codec;
    /**
     * Dubbo URL
     */
    private final URL url;

    /**
     * NettyClient or NettyServer
     */
    private final com.alibaba.dubbo.remoting.ChannelHandler handler;

    public NettyCodecAdapter(Codec2 codec, URL url, com.alibaba.dubbo.remoting.ChannelHandler handler) {
        this.codec = codec;
        this.url = url;
        this.handler = handler;
    }

    public ChannelHandler getEncoder() {
        return encoder;
    }

    public ChannelHandler getDecoder() {
        return decoder;
    }

    /**
     * 实现自定义的编码器
     * 由于dubbo中是通过netty处理的，又要有dubbo的自己的编码方式，需要继承netty中的编码类
     * 这样就可以通过netty调用，具体的实现细节通过dubbo中的编码方式去实现
     * InternalEncoder相当于dubbo中的codec的装饰者
     */
    private class InternalEncoder extends MessageToByteEncoder {

        @Override
        protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
            //封装netty的ByteBuf为dubbo的NettyBackedChannelBuffer
            com.alibaba.dubbo.remoting.buffer.ChannelBuffer buffer = new NettyBackedChannelBuffer(out);
            //获取netty的channel
            Channel ch = ctx.channel();
            //包装为dubbo的NettyChannel
            NettyChannel channel = NettyChannel.getOrAddChannel(ch, url, handler);
            try {
                codec.encode(channel, buffer, msg);
            } finally {
                // 移除 NettyChannel 对象，若断开连接
                NettyChannel.removeChannelIfDisconnected(ch);
            }
        }
    }

    /**
     * 实现自定义解码器
     * 解码
     */
    private class InternalDecoder extends ByteToMessageDecoder {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf input, List<Object> out) throws Exception {
           //封装netty的ByteBuf为dubbo的NettyBackedChannelBuffer
            ChannelBuffer message = new NettyBackedChannelBuffer(input);
            //包装为dubbo的NettyChannel
            NettyChannel channel = NettyChannel.getOrAddChannel(ctx.channel(), url, handler);

            Object msg;

            int saveReaderIndex;

            try {
                // decode object.
                // 循环解析，直到结束
                do {
                    //获取当前读取的位置
                    saveReaderIndex = message.readerIndex();
                    try {
                        // 解码
                        msg = codec.decode(channel, message);
                    } catch (IOException e) {
                        throw e;
                    }
                    // 需要更多输入，即消息不完整，标记回原有读进度，并结束
                    if (msg == Codec2.DecodeResult.NEED_MORE_INPUT) {
                        message.readerIndex(saveReaderIndex);
                        break;
                        // 解码到消息，添加到 `out`
                    } else {
                        //is it possible to go here ?  不可能
                        if (saveReaderIndex == message.readerIndex()) {
                            throw new IOException("Decode without read data.");
                        }
                        if (msg != null) {
                            out.add(msg);
                        }
                    }
                } while (message.readable());
            } finally {
                NettyChannel.removeChannelIfDisconnected(ctx.channel());
            }
        }
    }
}
