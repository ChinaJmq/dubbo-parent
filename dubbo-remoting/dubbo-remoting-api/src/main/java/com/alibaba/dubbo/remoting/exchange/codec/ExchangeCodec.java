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
package com.alibaba.dubbo.remoting.exchange.codec;

import com.alibaba.dubbo.common.io.Bytes;
import com.alibaba.dubbo.common.io.StreamUtils;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.serialize.Cleanable;
import com.alibaba.dubbo.common.serialize.ObjectInput;
import com.alibaba.dubbo.common.serialize.ObjectOutput;
import com.alibaba.dubbo.common.serialize.Serialization;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.buffer.ChannelBuffer;
import com.alibaba.dubbo.remoting.buffer.ChannelBufferInputStream;
import com.alibaba.dubbo.remoting.buffer.ChannelBufferOutputStream;
import com.alibaba.dubbo.remoting.exchange.Request;
import com.alibaba.dubbo.remoting.exchange.Response;
import com.alibaba.dubbo.remoting.exchange.support.DefaultFuture;
import com.alibaba.dubbo.remoting.telnet.codec.TelnetCodec;
import com.alibaba.dubbo.remoting.transport.CodecSupport;
import com.alibaba.dubbo.remoting.transport.ExceedPayloadLimitException;
import oracle.jrockit.jfr.events.Bits;

import java.io.IOException;
import java.io.InputStream;

/**
 * ExchangeCodec.
 *
 *
 *
 */
public class ExchangeCodec extends TelnetCodec {

    // header length.
    //HEADER_LENGTH 静态属性，Header 总长度，16 Bytes = 128 Bits
    protected static final int HEADER_LENGTH = 16;
    // magic header.
    protected static final short MAGIC = (short) 0xdabb;//short 占用2*8=16bits
    protected static final byte MAGIC_HIGH = Bytes.short2bytes(MAGIC)[0];//8bits
    protected static final byte MAGIC_LOW = Bytes.short2bytes(MAGIC)[1];//8bits
    protected static final byte FLAG_TWOWAY = (byte) 0x40;// 64
    // message flag.
    protected static final byte FLAG_REQUEST = (byte) 0x80;// 128  10000000
    protected static final byte FLAG_EVENT = (byte) 0x20;// 32 00100000
    protected static final int SERIALIZATION_MASK = 0x1f;// 31  00011111
    private static final Logger logger = LoggerFactory.getLogger(ExchangeCodec.class);

    public Short getMagicCode() {
        return MAGIC;
    }

    /**
     * 编码
     * @param channel
     * @param buffer
     * @param msg
     * @throws IOException
     */
    @Override
    public void encode(Channel channel, ChannelBuffer buffer, Object msg) throws IOException {
        // 请求
        if (msg instanceof Request) {
            encodeRequest(channel, buffer, (Request) msg);
        // 响应
        } else if (msg instanceof Response) {
            encodeResponse(channel, buffer, (Response) msg);
        // 提交给父类( Telnet ) 处理，目前是 Telnet 命令的结果。
        } else {
            super.encode(channel, buffer, msg);
        }
    }

    /**
     * 解码
     * @param channel
     * @param buffer
     * @return
     * @throws IOException
     */
    @Override
    public Object decode(Channel channel, ChannelBuffer buffer) throws IOException {
        // 读取buffer可读字节的长度
        int readable = buffer.readableBytes();
        byte[] header = new byte[Math.min(readable, HEADER_LENGTH)];
        buffer.readBytes(header);
        // 解码
        return decode(channel, buffer, readable, header);
    }

    @Override
    protected Object decode(Channel channel, ChannelBuffer buffer, int readable, byte[] header) throws IOException {
        // 非 Dubbo 协议，目前是 Telnet 命令。
        // check magic number.
        if (readable > 0 && header[0] != MAGIC_HIGH
                || readable > 1 && header[1] != MAGIC_LOW) {
            // 将 buffer 完全复制到 `header` 数组中。因为，上面的 `#decode(channel, buffer)` 方法，可能未读全
            int length = header.length;
            if (header.length < readable) {
                header = Bytes.copyOf(header, readable);
                buffer.readBytes(header, length, readable - length);
            }
            // 【TODO 8026 】header[i] == MAGIC_HIGH && header[i + 1] == MAGIC_LOW ？
            for (int i = 1; i < header.length - 1; i++) {
                if (header[i] == MAGIC_HIGH && header[i + 1] == MAGIC_LOW) {
                    buffer.readerIndex(buffer.readerIndex() - header.length + i);
                    header = Bytes.copyOf(header, i);
                    break;
                }
            }
            //提交给父类( Telnet ) 处理，目前是 Telnet 命令。
            return super.decode(channel, buffer, readable, header);
        }
        // Header 长度不够，返回需要更多的输入
        // check length.
        if (readable < HEADER_LENGTH) {
            return DecodeResult.NEED_MORE_INPUT;
        }
        // `[96 - 127]`：Body 的**长度**。通过该长度，读取 Body 。
        // get data length.
        int len = Bytes.bytes2int(header, 12);
        //验证Body的长度是否合理
        checkPayload(channel, len);
        // 需要可读字节的总长度
        int tt = len + HEADER_LENGTH;
        //可读字节不够，返回需要更多的输入
        if (readable < tt) {
            return DecodeResult.NEED_MORE_INPUT;
        }
        //基于消息长度的方式，拆包
        // 解析 Header + Body
        // limit input stream.
        ChannelBufferInputStream is = new ChannelBufferInputStream(buffer, len);

        try {
            return decodeBody(channel, is, header);
        } finally {
            // skip 未读完的流，并打印错误日志
            if (is.available() > 0) {
                try {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Skip input stream " + is.available());
                    }
                    StreamUtils.skipUnusedStream(is);
                } catch (IOException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * 解析 Header + Body ，根据情况，返回 Request 或 Reponse
     * @param channel
     * @param is
     * @param header
     * @return
     * @throws IOException
     */
    protected Object decodeBody(Channel channel, InputStream is, byte[] header) throws IOException {
        //proto -> Serialization 编号
        byte flag = header[2], proto = (byte) (flag & SERIALIZATION_MASK);
        //根据Serialization的编号获取Serialization实例
        Serialization s = CodecSupport.getSerialization(channel.getUrl(), proto);
        //解码读取body
        ObjectInput in = s.deserialize(channel.getUrl(), is);
        // get request id.
        //请求id 编号
        long id = Bytes.bytes2long(header, 4);
        //处理响应
        if ((flag & FLAG_REQUEST) == 0) {
            // decode response.
            Response res = new Response(id);
            //否为事件。
            if ((flag & FLAG_EVENT) != 0) {
                res.setEvent(Response.HEARTBEAT_EVENT);
            }
            // get status.
            //设置响应状态
            byte status = header[3];
            res.setStatus(status);
            //响应结果正常
            if (status == Response.OK) {
                try {
                    Object data;
                    //心跳检测
                    if (res.isHeartbeat()) {
                        data = decodeHeartbeatData(channel, in);
                    //事件
                    } else if (res.isEvent()) {
                        data = decodeEventData(channel, in);
                        //正常的响应
                    } else {
                        data = decodeResponseData(channel, in, getRequestData(id));
                    }
                    //设置结果
                    res.setResult(data);
                } catch (Throwable t) {
                    //设置异常状态
                    res.setStatus(Response.CLIENT_ERROR);
                    //设置异常信息
                    res.setErrorMessage(StringUtils.toString(t));
                }
            } else {
                //设置异常信息
                res.setErrorMessage(in.readUTF());
            }
            return res;
        } else {
            //处理请求
            // decode request.
            Request req = new Request(id);
            //设置版本号
            req.setVersion("2.0.0");
            //设置是否需要响应
            req.setTwoWay((flag & FLAG_TWOWAY) != 0);
            //如果是事件，则设置心跳时间
            if ((flag & FLAG_EVENT) != 0) {
                req.setEvent(Request.HEARTBEAT_EVENT);
            }
            try {
                Object data;
                if (req.isHeartbeat()) {
                    data = decodeHeartbeatData(channel, in);
                } else if (req.isEvent()) {
                    data = decodeEventData(channel, in);
                } else {
                    data = decodeRequestData(channel, in);
                }
                //设置请求数据
                req.setData(data);
            } catch (Throwable t) {
                // bad request
                req.setBroken(true);
                req.setData(t);
            }
            return req;
        }
    }

    protected Object getRequestData(long id) {
        DefaultFuture future = DefaultFuture.getFuture(id);
        if (future == null)
            return null;
        Request req = future.getRequest();
        if (req == null)
            return null;
        return req.getData();
    }

    protected void encodeRequest(Channel channel, ChannelBuffer buffer, Request req) throws IOException {
        //也是根据SPI扩展来获取，url中没指定的话默认使用hessian2
        Serialization serialization = getSerialization(channel);
        // header.
        //长度为16字节的数组，协议头
        byte[] header = new byte[HEADER_LENGTH];
        // `[0, 15]`：Magic Number
        // set magic number.
        //魔数0xdabb
        Bytes.short2bytes(MAGIC, header);

        // set request and serialization flag.
        // `[16, 20]`：Serialization 编号 && `[23]`：请求
        header[2] = (byte) (FLAG_REQUEST | serialization.getContentTypeId());
        // `[21]`：`event` 是否为事件。
        if (req.isTwoWay()) header[2] |= FLAG_TWOWAY;
        // `[22]`：`twoWay` 是否需要响应。
        if (req.isEvent()) header[2] |= FLAG_EVENT;
        // `[32 - 95]`：`id` 编号，Long 型。
        // set request id.
        Bytes.long2bytes(req.getId(), header, 4);

        // 编码 `Request.data` 到 Body ，并写入到 Buffer
        // encode request data.
        //获取buffer的写入位置
        int savedWriteIndex = buffer.writerIndex();
        //需要再加上协议头的长度之后，才是正确的写入位置
        buffer.writerIndex(savedWriteIndex + HEADER_LENGTH);
        ChannelBufferOutputStream bos = new ChannelBufferOutputStream(buffer);
        //使用 Serialization 序列化 Request.data ，写入到 Buffer 中
        ObjectOutput out = serialization.serialize(channel.getUrl(), bos);
        //处理事件
        if (req.isEvent()) {
            encodeEventData(channel, out, req.getData());
        //普通的请求
        } else {
            encodeRequestData(channel, out, req.getData());
        }
        // 释放资源
        out.flushBuffer();
        if (out instanceof Cleanable) {
            ((Cleanable) out).cleanup();
        }
        bos.flush();
        bos.close();
        // 检查 Body 长度，是否超过消息上限
        int len = bos.writtenBytes();
        checkPayload(channel, len);
        // `[96 - 127]`：Body 的**长度**。
        Bytes.int2bytes(len, header, 12);

        // 写入 Header 到 Buffer
        // write
        //重置写入的位置
        buffer.writerIndex(savedWriteIndex);
        //向buffer中写入消息头
        buffer.writeBytes(header); // write header.
        //buffer写出去的位置从writerIndex开始，加上header长度，加上数据长度
        buffer.writerIndex(savedWriteIndex + HEADER_LENGTH + len);
    }
    /**
     * 编码响应
     *
     * @param channel 通道
     * @param buffer Buffer
     * @param res 响应
     * @throws IOException 当发生 IO 异常时
     */
    protected void encodeResponse(Channel channel, ChannelBuffer buffer, Response res) throws IOException {
        int savedWriteIndex = buffer.writerIndex();
        try {
            Serialization serialization = getSerialization(channel);
            // header.
            byte[] header = new byte[HEADER_LENGTH];
            // set magic number.
            // `[0, 15]`：Magic Number
            // header.
            Bytes.short2bytes(MAGIC, header);
            // set request and serialization flag.
            header[2] = serialization.getContentTypeId();
            if (res.isHeartbeat()) header[2] |= FLAG_EVENT;
            // set response status.
            byte status = res.getStatus();
            header[3] = status;
            // set request id.
            Bytes.long2bytes(res.getId(), header, 4);

            buffer.writerIndex(savedWriteIndex + HEADER_LENGTH);
            ChannelBufferOutputStream bos = new ChannelBufferOutputStream(buffer);
            ObjectOutput out = serialization.serialize(channel.getUrl(), bos);
            // encode response data or error message.
            if (status == Response.OK) {
                if (res.isHeartbeat()) {
                    encodeHeartbeatData(channel, out, res.getResult());
                } else {
                    encodeResponseData(channel, out, res.getResult());
                }
            } else out.writeUTF(res.getErrorMessage());
            out.flushBuffer();
            if (out instanceof Cleanable) {
                ((Cleanable) out).cleanup();
            }
            bos.flush();
            bos.close();

            int len = bos.writtenBytes();
            checkPayload(channel, len);
            Bytes.int2bytes(len, header, 12);
            // write
            buffer.writerIndex(savedWriteIndex);
            buffer.writeBytes(header); // write header.
            buffer.writerIndex(savedWriteIndex + HEADER_LENGTH + len);
        } catch (Throwable t) {
            // clear buffer
            // 重置写入进度，下面新的 Response 需要用到。
            buffer.writerIndex(savedWriteIndex);
            // send error message to Consumer, otherwise, Consumer will wait till timeout.
            if (!res.isEvent() && res.getStatus() != Response.BAD_RESPONSE) {
                Response r = new Response(res.getId(), res.getVersion());
                r.setStatus(Response.BAD_RESPONSE);
                // 过长异常
                if (t instanceof ExceedPayloadLimitException) {
                    logger.warn(t.getMessage(), t);
                    try {
                        r.setErrorMessage(t.getMessage());
                        channel.send(r);
                        return;
                    } catch (RemotingException e) {
                        logger.warn("Failed to send bad_response info back: " + t.getMessage() + ", cause: " + e.getMessage(), e);
                    }
                    // 其他异常
                } else {
                    // FIXME log error message in Codec and handle in caught() of IoHanndler?
                    logger.warn("Fail to encode response: " + res + ", send bad_response info instead, cause: " + t.getMessage(), t);
                    try {
                        r.setErrorMessage("Failed to send response: " + res + ", cause: " + StringUtils.toString(t));
                        channel.send(r);
                        return;
                    } catch (RemotingException e) {
                        logger.warn("Failed to send bad_response info back: " + res + ", cause: " + e.getMessage(), e);
                    }
                }
            }

            // Rethrow exception
            if (t instanceof IOException) {
                throw (IOException) t;
            } else if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else if (t instanceof Error) {
                throw (Error) t;
            } else {
                throw new RuntimeException(t.getMessage(), t);
            }
        }
    }

    @Override
    protected Object decodeData(ObjectInput in) throws IOException {
        return decodeRequestData(in);
    }

    @Deprecated
    protected Object decodeHeartbeatData(ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    protected Object decodeRequestData(ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    protected Object decodeResponseData(ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    @Override
    protected void encodeData(ObjectOutput out, Object data) throws IOException {
        encodeRequestData(out, data);
    }

    private void encodeEventData(ObjectOutput out, Object data) throws IOException {
        out.writeObject(data);
    }

    @Deprecated
    protected void encodeHeartbeatData(ObjectOutput out, Object data) throws IOException {
        encodeEventData(out, data);
    }

    protected void encodeRequestData(ObjectOutput out, Object data) throws IOException {
        out.writeObject(data);
    }

    protected void encodeResponseData(ObjectOutput out, Object data) throws IOException {
        out.writeObject(data);
    }

    @Override
    protected Object decodeData(Channel channel, ObjectInput in) throws IOException {
        return decodeRequestData(channel, in);
    }

    protected Object decodeEventData(Channel channel, ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    @Deprecated
    protected Object decodeHeartbeatData(Channel channel, ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    protected Object decodeRequestData(Channel channel, ObjectInput in) throws IOException {
        return decodeRequestData(in);
    }

    protected Object decodeResponseData(Channel channel, ObjectInput in) throws IOException {
        return decodeResponseData(in);
    }

    protected Object decodeResponseData(Channel channel, ObjectInput in, Object requestData) throws IOException {
        return decodeResponseData(channel, in);
    }

    @Override
    protected void encodeData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(channel, out, data);
    }

    private void encodeEventData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeEventData(out, data);
    }

    @Deprecated
    protected void encodeHeartbeatData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeHeartbeatData(out, data);
    }

    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(out, data);
    }

    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeResponseData(out, data);
    }

}
