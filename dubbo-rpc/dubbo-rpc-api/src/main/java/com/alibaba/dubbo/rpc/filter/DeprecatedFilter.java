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
package com.alibaba.dubbo.rpc.filter;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.extension.Activate;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ConcurrentHashSet;
import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcException;

import java.util.Set;

/**
 * DeprecatedInvokerFilter
 * 用于服务消费者中，通过 <dubbo: service /> 或 <dubbo:reference /> 或 <dubbo:method /> 的 "deprecated" 配置项为 true 来开启
 * 服务是否过时，如果设为true，消费方引用时将打印服务过时警告error日志
 *
 */
@Activate(group = Constants.CONSUMER, value = Constants.DEPRECATED_KEY)
public class DeprecatedFilter implements Filter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeprecatedFilter.class);
    /**
     7:      * 已经打印日志的方法集合
     8:      */
    private static final Set<String> logged = new ConcurrentHashSet<String>();

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // 获得服务方法名
        String key = invoker.getInterface().getName() + "." + invocation.getMethodName();
        // 打印告警日志，仅打印一次
        if (!logged.contains(key)) {
            logged.add(key);
            if (invoker.getUrl().getMethodParameter(invocation.getMethodName(), Constants.DEPRECATED_KEY, false)) {
                //  [14/04/18 11:51:35:035 CST] main ERROR filter.DeprecatedFilter:  [DUBBO] The service method com.alibaba.dubbo.demo.DemoService.say01(String) is DEPRECATED! Declare from dubbo://192.168.3.17:20880/com.alibaba.dubbo.demo.DemoService?accesslog=true&anyhost=true&application=demo-consumer&callbacks=1000&check=false&client=netty4&default.delay=-1&default.retries=0&delay=-1&deprecated=false&dubbo=2.0.0&generic=false&interface=com.alibaba.dubbo.demo.DemoService&methods=sayHello,callbackParam,say03,say04,say01,bye,say02&payload=1000&pid=16820&qos.port=33333&register.ip=192.168.3.17&remote.timestamp=1523720843597&say01.deprecated=true&sayHello.async=true&server=netty4&service.filter=demo&side=consumer&timeout=100000&timestamp=1523721049491, dubbo version: 2.0.0, current host: 192.168.3.17
                LOGGER.error("The service method " + invoker.getInterface().getName() + "." + getMethodSignature(invocation) + " is DEPRECATED! Declare from " + invoker.getUrl());
            }
        }
        return invoker.invoke(invocation);
    }

    private String getMethodSignature(Invocation invocation) {
        StringBuilder buf = new StringBuilder(invocation.getMethodName());
        buf.append("(");
        Class<?>[] types = invocation.getParameterTypes();
        if (types != null && types.length > 0) {
            boolean first = true;
            for (Class<?> type : types) {
                if (first) {
                    first = false;
                } else {
                    buf.append(", ");
                }
                buf.append(type.getSimpleName());
            }
        }
        buf.append(")");
        return buf.toString();
    }

}
