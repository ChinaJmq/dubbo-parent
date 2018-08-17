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
import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.filter.tps.DefaultTPSLimiter;
import com.alibaba.dubbo.rpc.filter.tps.TPSLimiter;

/**
 * Limit TPS for either service or service's particular method
 * 用于服务提供者中，提供 限流 的功能
 *
 *  通过 <dubbo:parameter key="tps" value="" /> 配置项，添加到 <dubbo:service /> 或 <dubbo:provider /> 或 <dubbo:protocol /> 中开启
 *
 *  <dubbo:service interface="com.alibaba.dubbo.demo.DemoService" ref="demoServiceImpl" protocol="injvm" >
 *      <dubbo:parameter key="tps" value="100" />
 *  </dubbo:service>
 *
 *  或者
 *  通过 <dubbo:parameter key="tps.interval" value="" /> 配置项，设置 TPS 周期。
 *
 *
 *  目前暂未配置 TpsLimitFilter 到 Dubbo SPI 文件里，所以我们需要添加到 com.alibaba.dubbo.rpc.Filter 中
 *  tps=com.alibaba.dubbo.rpc.filter.TpsLimitFilter
 *
 *  实际在服务的限流时，更推荐使用 令牌桶算法,具体参见http://www.iocoder.cn/Eureka/rate-limiter/?self
 */
@Activate(group = Constants.PROVIDER, value = Constants.TPS_LIMIT_RATE_KEY)
public class TpsLimitFilter implements Filter {

    private final TPSLimiter tpsLimiter = new DefaultTPSLimiter();

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {

        //调用 TPSLimiter#isAllowable(url, invocation) 方法，
        // 根据 tps 限流规则判断是否限制此次调用。若是，抛出 RpcException 异常。目前使用 TPSLimiter 作为限流器的实现类
        if (!tpsLimiter.isAllowable(invoker.getUrl(), invocation)) {
            throw new RpcException(
                    "Failed to invoke service " +
                            invoker.getInterface().getName() +
                            "." +
                            invocation.getMethodName() +
                            " because exceed max service tps.");
        }

        return invoker.invoke(invocation);
    }

}
