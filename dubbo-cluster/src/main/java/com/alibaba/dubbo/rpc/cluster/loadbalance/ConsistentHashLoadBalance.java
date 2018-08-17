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
package com.alibaba.dubbo.rpc.cluster.loadbalance;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ConsistentHashLoadBalance
 * 一致性 Hash，相同参数的请求总是发到同一提供者。
 * 当某一台提供者挂时，原本发往该提供者的请求，基于虚拟节点，平摊到其它提供者，不会引起剧烈变动。
 *
 */
public class ConsistentHashLoadBalance extends AbstractLoadBalance {
    /**
     4:      * 服务方法与一致性哈希选择器的映射
     5:      *
     6:      * KEY：serviceKey + "." + methodName
     7:      */
    private final ConcurrentMap<String, ConsistentHashSelector<?>> selectors = new ConcurrentHashMap<String, ConsistentHashSelector<?>>();

    @SuppressWarnings("unchecked")
    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        //key为方法名称，例如com.alibaba.dubbo.demo.TestService.getRandomNumber
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        // 基于 invokers 集合，根据对象内存地址来计算定义哈希值
        int identityHashCode = System.identityHashCode(invokers);
        // 获得 ConsistentHashSelector 对象。若为空，或者定义哈希值变更（说明 invokers 集合发生变化），进行创建新的 ConsistentHashSelector 对象
        ConsistentHashSelector<T> selector = (ConsistentHashSelector<T>) selectors.get(key);
        if (selector == null || selector.identityHashCode != identityHashCode) {
            selectors.put(key, new ConsistentHashSelector<T>(invokers, invocation.getMethodName(), identityHashCode));
            selector = (ConsistentHashSelector<T>) selectors.get(key);
        }
        //调用 ConsistentHashSelector#select(invocation) 方法，选择一个 Invoker 对象。
        return selector.select(invocation);
    }

    /**
     * 是 ConsistentHashLoadBalance 的私有的静态内部类，一致性哈希选择器，基于 Ketama 算法。
     * 具体参见http://langyu.iteye.com/blog/684087
     * @param <T>
     */
    private static final class ConsistentHashSelector<T> {
        /**
         * 虚拟节点与 Invoker 的映射关系
         */
        private final TreeMap<Long, Invoker<T>> virtualInvokers;
        /**
         * 每个Invoker 对应的虚拟节点数
         */
        private final int replicaNumber;
        /**
         * 定义哈希值
         */
        private final int identityHashCode;
        /**
         * 取值参数位置数组
         */
        private final int[] argumentIndex;

        ConsistentHashSelector(List<Invoker<T>> invokers, String methodName, int identityHashCode) {
            this.virtualInvokers = new TreeMap<Long, Invoker<T>>();
            // 设置 identityHashCode
            this.identityHashCode = identityHashCode;
            URL url = invokers.get(0).getUrl();
            //缺省用 160 份虚拟节点，如果要修改，请配置 <dubbo:parameter key="hash.nodes" value="320" />
            this.replicaNumber = url.getMethodParameter(methodName, "hash.nodes", 160);
            //缺省只对第一个参数 Hash，如果要修改，请配置 <dubbo:parameter key="hash.arguments" value="0,1" />
            String[] index = Constants.COMMA_SPLIT_PATTERN.split(url.getMethodParameter(methodName, "hash.arguments", "0"));
            //初始化参数数组
            argumentIndex = new int[index.length];
            //将index的值赋值到argumentIndex中
            for (int i = 0; i < index.length; i++) {
                argumentIndex[i] = Integer.parseInt(index[i]);
            }
            for (Invoker<T> invoker : invokers) {
                String address = invoker.getUrl().getAddress();
                // 每四个虚拟结点为一组，为什么这样？下面会说到
                for (int i = 0; i < replicaNumber / 4; i++) {
                    // 这组虚拟结点得到惟一名称
                    byte[] digest = md5(address + i);
                    // Md5是一个16字节长度的数组，将16字节的数组每四个字节一组，分别对应一个虚拟结点，这就是为什么上面把虚拟结点四个划分一组的原因
                    for (int h = 0; h < 4; h++) {
                        // 对于每四个字节，组成一个long值数值，做为这个虚拟节点的在环中的惟一key
                        long m = hash(digest, h);
                        virtualInvokers.put(m, invoker);
                    }
                }
            }
        }

        /**
         * 获取Invoker
         * @param invocation
         * @return
         */
        public Invoker<T> select(Invocation invocation) {
            // 基于方法参数，获得 KEY
            String key = toKey(invocation.getArguments());
            // 计算 MD5 值
            byte[] digest = md5(key);
            return selectForKey(hash(digest, 0));
        }

        /**
         * 基于方法参数，获得 KEY
         * @param args
         * @return
         */
        private String toKey(Object[] args) {
            StringBuilder buf = new StringBuilder();
            for (int i : argumentIndex) {
                if (i >= 0 && i < args.length) {
                    buf.append(args[i]);
                }
            }
            return buf.toString();
        }

        private Invoker<T> selectForKey(long hash) {
            // 得到大于等于当前 key 的那个子 Map ，然后从中取出第一个 key ，就是大于且离它最近的那个 key
            Map.Entry<Long, Invoker<T>> entry = virtualInvokers.tailMap(hash, true).firstEntry();
            // 不存在，则取 virtualInvokers 第一个
        	if (entry == null) {
        		entry = virtualInvokers.firstEntry();
        	}
            // 存在，则返回
        	return entry.getValue();
        }

        private long hash(byte[] digest, int number) {
            return (((long) (digest[3 + number * 4] & 0xFF) << 24)
                    | ((long) (digest[2 + number * 4] & 0xFF) << 16)
                    | ((long) (digest[1 + number * 4] & 0xFF) << 8)
                    | (digest[number * 4] & 0xFF))
                    & 0xFFFFFFFFL;
        }

        /**
         * 计算 MD5
         * @param value
         * @return
         */
        private byte[] md5(String value) {
            MessageDigest md5;
            try {
                md5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            md5.reset();
            byte[] bytes;
            try {
                bytes = value.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            md5.update(bytes);
            return md5.digest();
        }

    }

}
