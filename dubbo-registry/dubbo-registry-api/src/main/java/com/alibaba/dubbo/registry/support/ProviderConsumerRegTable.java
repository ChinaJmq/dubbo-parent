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
package com.alibaba.dubbo.registry.support;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.ConcurrentHashSet;
import com.alibaba.dubbo.registry.integration.RegistryDirectory;
import com.alibaba.dubbo.rpc.Invoker;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @date 2017/11/23
 * 服务提供者和消费者注册表，存储 JVM 进程内自己的服务提供者和消费者的 Invoker
 */
public class ProviderConsumerRegTable {
    /**
     * 服务提供者 Invoker 集合
     *
     * key：服务提供者 URL 服务键
     */
    public static ConcurrentHashMap<String, Set<ProviderInvokerWrapper>> providerInvokers = new ConcurrentHashMap<String, Set<ProviderInvokerWrapper>>();
    /**
     * 服务消费者 Invoker 集合
     *
     * key：服务消费者 URL 服务键
     */
    public static ConcurrentHashMap<String, Set<ConsumerInvokerWrapper>> consumerInvokers = new ConcurrentHashMap<String, Set<ConsumerInvokerWrapper>>();
    /**
     * 注册 Provider Invoker
     *
     * @param invoker invoker 对象
     * @param registryUrl 注册中心 URL
     * @param providerUrl 服务提供者 URL
     */
    public static void registerProvider(Invoker invoker, URL registryUrl, URL providerUrl) {
        //创建ProviderInvokerWrapper
        ProviderInvokerWrapper wrapperInvoker = new ProviderInvokerWrapper(invoker, registryUrl, providerUrl);
        //创建服务提供者的唯一服务键
        String serviceUniqueName = providerUrl.getServiceKey();
        //根据服务name去服务提供表中查找
        Set<ProviderInvokerWrapper> invokers = providerInvokers.get(serviceUniqueName);
        //没有则创建
        if (invokers == null) {
            providerInvokers.putIfAbsent(serviceUniqueName, new ConcurrentHashSet<ProviderInvokerWrapper>());
            invokers = providerInvokers.get(serviceUniqueName);
        }
        //将ProviderInvokerWrapper放进注册表中
        invokers.add(wrapperInvoker);
    }
    /**
     * 获得指定服务键的 Provider Invoker 集合
     *
     * @param serviceUniqueName 服务键
     * @return 集合
     */
    public static Set<ProviderInvokerWrapper> getProviderInvoker(String serviceUniqueName) {
        Set<ProviderInvokerWrapper> invokers = providerInvokers.get(serviceUniqueName);
        if (invokers == null) {
            return Collections.emptySet();
        }
        return invokers;
    }

    /**
     * 获得服务提供者对应的 Invoker Wrapper 对象
     *
     * @param invoker 服务提供者 Invoker
     * @return Invoker Wrapper 对象
     */
    public static ProviderInvokerWrapper getProviderWrapper(Invoker invoker) {
        //获取URL
        URL providerUrl = invoker.getUrl();
        //如果是注册中心的URL，则获取具体提供者的URL
        if (Constants.REGISTRY_PROTOCOL.equals(providerUrl.getProtocol())) {
            providerUrl = URL.valueOf(providerUrl.getParameterAndDecoded(Constants.EXPORT_KEY));
        }
        //创建服务提供者的唯一服务name
        String serviceUniqueName = providerUrl.getServiceKey();
        //获取ProviderInvokerWrapper集合
        Set<ProviderInvokerWrapper> invokers = providerInvokers.get(serviceUniqueName);
        if (invokers == null) {
            return null;
        }
        //遍历获取具体的ProviderInvokerWrapper
        for (ProviderInvokerWrapper providerWrapper : invokers) {
            Invoker providerInvoker = providerWrapper.getInvoker();
            if (providerInvoker == invoker) {
                return providerWrapper;
            }
        }

        return null;
    }
    /**
     * 注册 Consumer Invoker
     *
     * @param invoker invoker 对象
     * @param registryUrl 注册中心 URL
     * @param consumerUrl 服务消费者 URL
     * @param registryDirectory 注册中心 Directory
     */
    public static void registerConsumer(Invoker invoker, URL registryUrl, URL consumerUrl, RegistryDirectory registryDirectory) {
        // 创建 ConsumerInvokerWrapper 对象
        ConsumerInvokerWrapper wrapperInvoker = new ConsumerInvokerWrapper(invoker, registryUrl, consumerUrl, registryDirectory);
        // 服务键
        String serviceUniqueName = consumerUrl.getServiceKey();
        // 添加到集合
        Set<ConsumerInvokerWrapper> invokers = consumerInvokers.get(serviceUniqueName);
        if (invokers == null) {
            consumerInvokers.putIfAbsent(serviceUniqueName, new ConcurrentHashSet<ConsumerInvokerWrapper>());
            invokers = consumerInvokers.get(serviceUniqueName);
        }
        invokers.add(wrapperInvoker);
    }
    /**
     * 获得指定服务键的 Consumer Invoker 集合
     *
     * @param serviceUniqueName 服务键
     * @return 集合
     */
    public static Set<ConsumerInvokerWrapper> getConsumerInvoker(String serviceUniqueName) {
        Set<ConsumerInvokerWrapper> invokers = consumerInvokers.get(serviceUniqueName);
        if (invokers == null) {
            return Collections.emptySet();
        }
        return invokers;
    }

}