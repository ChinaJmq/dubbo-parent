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

package com.alibaba.dubbo.remoting.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public abstract class AbstractChannelBuffer implements ChannelBuffer {
    /**
     * 读取位置
     */
    private int readerIndex;
    /**
     * 写入位置
     */
    private int writerIndex;
    /**
     * 标记的读取位置
     */
    private int markedReaderIndex;
    /**
     * 标记的写入位置
     */
    private int markedWriterIndex;

    /**
     * 获取读取的位置
     * @return
     */
    @Override
    public int readerIndex() {
        return readerIndex;
    }

    /**
     * 设置读取的位置
     * @param readerIndex
     */
    @Override
    public void readerIndex(int readerIndex) {
        if (readerIndex < 0 || readerIndex > writerIndex) {
            throw new IndexOutOfBoundsException();
        }
        this.readerIndex = readerIndex;
    }

    /**
     * 获取写入的位置
     * @return
     */
    @Override
    public int writerIndex() {
        return writerIndex;
    }

    /**
     * 设置写入的位置
     * @param writerIndex
     */
    @Override
    public void writerIndex(int writerIndex) {
        if (writerIndex < readerIndex || writerIndex > capacity()) {
            throw new IndexOutOfBoundsException();
        }
        this.writerIndex = writerIndex;
    }

    /**
     * 设置读取和写入的位置
     * @param readerIndex
     * @param writerIndex
     */
    @Override
    public void setIndex(int readerIndex, int writerIndex) {
        if (readerIndex < 0 || readerIndex > writerIndex || writerIndex > capacity()) {
            throw new IndexOutOfBoundsException();
        }
        this.readerIndex = readerIndex;
        this.writerIndex = writerIndex;
    }

    /**
     * 该操作不会清空缓冲区内容本身，其主要是为了操作位置指针，
     * 将readerIndex和writerIndex重置为0
     */
    @Override
    public void clear() {
        readerIndex = writerIndex = 0;
    }

    /**
     * 是否可读
     * @return
     */
    @Override
    public boolean readable() {
        return readableBytes() > 0;
    }

    /**
     * 是否可写
     * @return
     */
    @Override
    public boolean writable() {
        return writableBytes() > 0;
    }

    /**
     * 获取可读字节数
     * @return
     */
    @Override
    public int readableBytes() {
        return writerIndex - readerIndex;
    }

    /**
     * 获取当前可写字节数
     * @return
     */
    @Override
    public int writableBytes() {
        return capacity() - writerIndex;
    }

    /**
     * 将当前的readerIndex备份到markedReaderIndex中
     */
    @Override
    public void markReaderIndex() {
        markedReaderIndex = readerIndex;
    }

    /**
     * 将当前的readerIndex重置为markedReaderIndex的值
     */
    @Override
    public void resetReaderIndex() {
        readerIndex(markedReaderIndex);
    }

    /**
     * 将当前的writerIndex备份到markedWriterIndex中
     */
    @Override
    public void markWriterIndex() {
        markedWriterIndex = writerIndex;
    }

    /**
     * 将当前的writerIndex重置为markedWriterIndex的值
     */
    @Override
    public void resetWriterIndex() {
        writerIndex = markedWriterIndex;
    }

    /**
     * 释放0到readerIndex之间已经读取的空间；同时复制readerIndex和writerIndex之间的数据到
     * 0到writerIndex-readerIndex之间；修改readerIndex和writerIndex的值。
     * 该操作会发生字节数据的内存复制，频繁调用会导致性能下降。
     * 此外，相比其他java对象，缓冲区的分配和释放是个耗时的操作，
     * 缓冲区的动态扩张需要进行进行字节数据的复制，也是耗时的操作，因此应尽量提高缓冲区的重用率
     */
    @Override
    public void discardReadBytes() {
        if (readerIndex == 0) {
            return;
        }
        setBytes(0, this, readerIndex, writerIndex - readerIndex);
        writerIndex -= readerIndex;
        markedReaderIndex = Math.max(markedReaderIndex - readerIndex, 0);
        markedWriterIndex = Math.max(markedWriterIndex - readerIndex, 0);
        readerIndex = 0;
    }

    /**
     * 确保有足够的空间可写入writableBytes的字节
     * @param writableBytes the expected minimum number of writable bytes
     */
    @Override
    public void ensureWritableBytes(int writableBytes) {
        if (writableBytes > writableBytes()) {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * 将此缓冲区的数据传输到以指定绝对值开始的指定目标index。
     * @param index
     * @param dst
     */
    @Override
    public void getBytes(int index, byte[] dst) {
        getBytes(index, dst, 0, dst.length);
    }

    /**
     * 将此缓冲区的数据传输到指定的目标位置，index直到目标位置变为不可写入。
     * @param index
     * @param dst
     */
    @Override
    public void getBytes(int index, ChannelBuffer dst) {
        getBytes(index, dst, dst.writableBytes());
    }

    /**
     * 将此缓冲区的数据传输到以指定绝对值开始的指定目标index。
     * @param index
     * @param dst
     * @param length the number of bytes to transfer
     */
    @Override
    public void getBytes(int index, ChannelBuffer dst, int length) {
        if (length > dst.writableBytes()) {
            throw new IndexOutOfBoundsException();
        }
        getBytes(index, dst, dst.writerIndex(), length);
        dst.writerIndex(dst.writerIndex() + length);
    }

    /**
     * 将指定的源数组的数据从指定的绝对值开始传送到此缓冲区index。
     * @param index
     * @param src
     */
    @Override
    public void setBytes(int index, byte[] src) {
        setBytes(index, src, 0, src.length);
    }

    /**
     * 将指定的源缓冲区的数据从指定的绝对值开始传输到此缓冲区，index直到源缓冲区变得不可读。
     * @param index
     * @param src
     */
    @Override
    public void setBytes(int index, ChannelBuffer src) {
        setBytes(index, src, src.readableBytes());
    }

    /**
     * 将指定的源缓冲区的数据从指定的绝对值开始传送到此缓冲区index。
     * @param index
     * @param src
     * @param length the number of bytes to transfer
     */
    @Override
    public void setBytes(int index, ChannelBuffer src, int length) {
        if (length > src.readableBytes()) {
            throw new IndexOutOfBoundsException();
        }
        setBytes(index, src, src.readerIndex(), length);
        src.readerIndex(src.readerIndex() + length);
    }

    /**
     * 在当前获取一个字节,readerIndex并在此缓冲区中增加readerIndexby 1。
     * @return
     */
    @Override
    public byte readByte() {
        if (readerIndex == writerIndex) {
            throw new IndexOutOfBoundsException();
        }
        return getByte(readerIndex++);
    }

    /**
     * 将当前ByteBuf中的数据读取到新创建的ByteBuf中，
     * 从readerIndex开始读取length字节的数据。返回的ByteBuf readerIndex 为0，writeIndex为length
     * @param length the number of bytes to transfer
     * @return
     */
    @Override
    public ChannelBuffer readBytes(int length) {
        checkReadableBytes(length);
        if (length == 0) {
            return ChannelBuffers.EMPTY_BUFFER;
        }
        ChannelBuffer buf = factory().getBuffer(length);
        buf.writeBytes(this, readerIndex, length);
        readerIndex += length;
        return buf;
    }

    /**
     * 将当前ByteBuf中的数据读取到目标ByteBuf （dst）中，
     * 从readerIndex开始读取，长度为length，从目标ByteBuf dstIndex开始写入数据。
     * 读取完成后，当前ByteBuf的readerIndex+=length，目标ByteBuf的writeIndex+=length
     * @param dst
     * @param dstIndex the first index of the destination
     * @param length   the number of bytes to transfer
     */
    @Override
    public void readBytes(byte[] dst, int dstIndex, int length) {
        checkReadableBytes(length);
        getBytes(readerIndex, dst, dstIndex, length);
        readerIndex += length;
    }

    /**
     * 将当前ByteBuf中的数据读取到byte数组dst中，
     * 从当前ByteBuf readerIndex开始读取，读取长度为dst.length，从byte数组dst索引0处开始写入数据
     * @param dst
     */
    @Override
    public void readBytes(byte[] dst) {
        readBytes(dst, 0, dst.length);
    }
    /**
     * 将当前ByteBuf中的数据读取到目标ByteBuf （dst）中，
     * 从当前ByteBuf readerIndex开始读取，直到目标ByteBuf无可写空间，从目标ByteBuf writeIndex开始写入数据。
     * 读取完成后，当前ByteBuf的readerIndex+=读取的字节数。目标ByteBuf的writeIndex+=读取的字节数。
     * @param dst
     */
    @Override
    public void readBytes(ChannelBuffer dst) {
        readBytes(dst, dst.writableBytes());
    }

    /**
     * 将当前ByteBuf中的数据读取到目标ByteBuf （dst）中，从当前ByteBuf readerIndex开始读取，长度为length，
     * 从目标ByteBuf writeIndex开始写入数据。
     * 读取完成后，当前ByteBuf的readerIndex+=length，目标ByteBuf的writeIndex+=length
     * @param dst
     * @param length
     */
    @Override
    public void readBytes(ChannelBuffer dst, int length) {
        if (length > dst.writableBytes()) {
            throw new IndexOutOfBoundsException();
        }
        readBytes(dst, dst.writerIndex(), length);
        dst.writerIndex(dst.writerIndex() + length);
    }

    /**
     * 将当前ByteBuf中的数据读取到目标ByteBuf （dst）中，从readerIndex开始读取，长度为length，
     * 从目标ByteBuf dstIndex开始写入数据。
     * 读取完成后，当前ByteBuf的readerIndex+=length，目标ByteBuf的writeIndex+=length
     * @param dst
     * @param dstIndex the first index of the destination
     * @param length   the number of bytes to transfer
     */
    @Override
    public void readBytes(ChannelBuffer dst, int dstIndex, int length) {
        checkReadableBytes(length);
        getBytes(readerIndex, dst, dstIndex, length);
        readerIndex += length;
    }

    /**
     * 将当前ByteBuf中的数据读取到ByteBuffer dst中，
     * 从当前ByteBuf readerIndex开始读取，直到dst的位置指针到达ByteBuffer 的limit。
     * 读取完成后，当前ByteBuf的readerIndex+=dst.remaining()
       throws IndexOutOfBoundsException:
     * @param dst
     */
    @Override
    public void readBytes(ByteBuffer dst) {
        int length = dst.remaining();
        checkReadableBytes(length);
        getBytes(readerIndex, dst);
        readerIndex += length;
    }

    /**
     * 将当前ByteBuf readerIndex读取数据到输出流OutputStream中，读取的字节长度为length
     * @param out
     * @param length the number of bytes to transfer
     * @throws IOException
     */
    @Override
    public void readBytes(OutputStream out, int length) throws IOException {
        checkReadableBytes(length);
        getBytes(readerIndex, out, length);
        readerIndex += length;
    }

    /**
     * 更新当前ByteBuf的readerIndex，更新后将跳过length字节的数据读取。
     * @param length
     */
    @Override
    public void skipBytes(int length) {
        int newReaderIndex = readerIndex + length;
        if (newReaderIndex > writerIndex) {
            throw new IndexOutOfBoundsException();
        }
        readerIndex = newReaderIndex;
    }

    /**
     * 将value写入到当前ByteBuf中。写入成功，writeIndex+=1
     * @param value
     */
    @Override
    public void writeByte(int value) {
        setByte(writerIndex++, value);
    }
    /**
     * 将源ByteBuf src中从srcIndex开始，长度length的可读字节写入到当前ByteBuf。
     * 从当前ByteBuf writeIndex写入数据。写入成功，writeIndex+=length
     * @param src
     */
    @Override
    public void writeBytes(byte[] src, int srcIndex, int length) {
        setBytes(writerIndex, src, srcIndex, length);
        writerIndex += length;
    }

    /**
     * 将源字节数组src中所有可读字节写入到当前ByteBuf。
     * 从当前ByteBuf writeIndex写入数据。写入成功，writeIndex+=src.length
     * @param src
     */
    @Override
    public void writeBytes(byte[] src) {
        writeBytes(src, 0, src.length);
    }

    /**
     * 将源ByteBuffer src中所有可读字节写入到当前ByteBuf。
     * 从当前ByteBuf writeIndex写入数据。写入成功，writeIndex+=src.remaining()
     * @param src
     */
    @Override
    public void writeBytes(ChannelBuffer src) {
        writeBytes(src, src.readableBytes());
    }

    /**
     * 将源ByteBuf src中从readerIndex开始，长度length的可读字节写入到当前ByteBuf。
     * 从当前ByteBuf writeIndex写入数据。写入成功，writeIndex+=length
     * @param src
     * @param length the number of bytes to transfer
     */
    @Override
    public void writeBytes(ChannelBuffer src, int length) {
        if (length > src.readableBytes()) {
            throw new IndexOutOfBoundsException();
        }
        writeBytes(src, src.readerIndex(), length);
        src.readerIndex(src.readerIndex() + length);
    }

    /**
     * 将源ByteBuf src中从srcIndex开始，长度length的可读字节写入到当前ByteBuf。
     * 从当前ByteBuf writeIndex写入数据。写入成功，writeIndex+=length
     * @param src
     * @param srcIndex the first index of the source
     * @param length   the number of bytes to transfer
     */
    @Override
    public void writeBytes(ChannelBuffer src, int srcIndex, int length) {
        setBytes(writerIndex, src, srcIndex, length);
        writerIndex += length;
    }

    /**
     * 将源ByteBuffer src中所有可读字节写入到当前ByteBuf。
     * 从当前ByteBuf writeIndex写入数据。写入成功，writeIndex+=src.remaining()
     * @param src
     */
    @Override
    public void writeBytes(ByteBuffer src) {
        int length = src.remaining();
        setBytes(writerIndex, src);
        writerIndex += length;
    }

    /**
     * 将源InputStream in中的内容写入到当前ByteBuf，写入的最大长度为length，实际写入的字节数可能少于length。
     * 从当前ByteBuf writeIndex写入数据。
     * 写入成功，writeIndex+=实际写入的字节数。返回实际写入的字节数
     * @param in
     * @param length the number of bytes to transfer
     * @return
     * @throws IOException
     */
    @Override
    public int writeBytes(InputStream in, int length) throws IOException {
        int writtenBytes = setBytes(writerIndex, in, length);
        if (writtenBytes > 0) {
            writerIndex += writtenBytes;
        }
        return writtenBytes;
    }

    /**
     * 从当前ByteBuf复制一个新的ByteBuf对象，复制的新对象缓冲区的内容和索引均是独立的。
     * 该操作不修改原ByteBuf的readerIndex和writerIndex。
     * （复制readerIndex到writerIndex之间的内容，其他属性与原ByteBuf相同，如maxCapacity，ByteBufAllocator）
     * @return
     */
    @Override
    public ChannelBuffer copy() {
        return copy(readerIndex, readableBytes());
    }

    @Override
    public ByteBuffer toByteBuffer() {
        return toByteBuffer(readerIndex, readableBytes());
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ChannelBuffer
                && ChannelBuffers.equals(this, (ChannelBuffer) o);
    }

    @Override
    public int compareTo(ChannelBuffer that) {
        return ChannelBuffers.compare(this, that);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '(' +
                "ridx=" + readerIndex + ", " +
                "widx=" + writerIndex + ", " +
                "cap=" + capacity() +
                ')';
    }

    /**
     * 校验读取的长度是否小于可读字节的长度
     * @param minimumReadableBytes
     */
    protected void checkReadableBytes(int minimumReadableBytes) {
        if (readableBytes() < minimumReadableBytes) {
            throw new IndexOutOfBoundsException();
        }
    }
}
