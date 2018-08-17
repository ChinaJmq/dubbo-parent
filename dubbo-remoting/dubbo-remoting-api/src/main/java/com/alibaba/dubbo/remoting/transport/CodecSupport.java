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
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.serialize.Serialization;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 编解码工具类，提供查询 Serialization 的功能
 */
public class CodecSupport {

    private static final Logger logger = LoggerFactory.getLogger(CodecSupport.class);
    /**
     * 序列化对象集合
     * key：序列化类型编号 {@link Serialization#getContentTypeId()}
     */
    private static Map<Byte, Serialization> ID_SERIALIZATION_MAP = new HashMap<Byte, Serialization>();
    /**
     * 序列化名集合
     * key：序列化类型编号 {@link Serialization#getContentTypeId()}
     * value: 序列化拓展名
     */
    private static Map<Byte, String> ID_SERIALIZATIONNAME_MAP = new HashMap<Byte, String>();

    static {
        // 基于 Dubbo SPI ，初始化,也就是所有的Serialization实现类的配置文件中的key的集合，如【fastjson,kryo】
        Set<String> supportedExtensions = ExtensionLoader.getExtensionLoader(Serialization.class).getSupportedExtensions();
        for (String name : supportedExtensions) {
            //获取具体的序列化实例
            Serialization serialization = ExtensionLoader.getExtensionLoader(Serialization.class).getExtension(name);
            //获取系列化实例的id编号
            byte idByte = serialization.getContentTypeId();
            //要确保不同的系列化的id唯一，如果有相同的记录错误日志，直接continue
            if (ID_SERIALIZATION_MAP.containsKey(idByte)) {
                logger.error("Serialization extension " + serialization.getClass().getName()
                        + " has duplicate id to Serialization extension "
                        + ID_SERIALIZATION_MAP.get(idByte).getClass().getName()
                        + ", ignore this Serialization extension");
                continue;
            }
            //放入系列化对象集合
            ID_SERIALIZATION_MAP.put(idByte, serialization);
            //放入系列化名集合
            ID_SERIALIZATIONNAME_MAP.put(idByte, name);
        }
    }

    private CodecSupport() {
    }

    /**
     * 根据id获取具体的系列化对象
     * @param id
     * @return
     */
    public static Serialization getSerializationById(Byte id) {
        return ID_SERIALIZATION_MAP.get(id);
    }

    /**
     * 基于spi 根据URL获取具体的系列化对象
     * @param url
     * @return
     */
    public static Serialization getSerialization(URL url) {
        return ExtensionLoader.getExtensionLoader(Serialization.class).getExtension(
                url.getParameter(Constants.SERIALIZATION_KEY, Constants.DEFAULT_REMOTING_SERIALIZATION));
    }

    /**
     *
     * @param url
     * @param id
     * @return
     * @throws IOException
     */
    public static Serialization getSerialization(URL url, Byte id) throws IOException {
        Serialization serialization = getSerializationById(id);
        String serializationName = url.getParameter(Constants.SERIALIZATION_KEY, Constants.DEFAULT_REMOTING_SERIALIZATION);
        // 出于安全的目的，针对 JDK 的序列化方式（对应编号为 3、4、7），检查连接到服务器的 URL 和实际传输的数据，协议是否一致。
        // Check if "serialization id" passed from network matches the id on this side(only take effect for JDK serialization), for security purpose.
        if (serialization == null
                || ((id == 3 || id == 7 || id == 4) && !(serializationName.equals(ID_SERIALIZATIONNAME_MAP.get(id))))) {
            throw new IOException("Unexpected serialization id:" + id + " received from network, please check if the peer send the right id.");
        }
        return serialization;
    }

}
