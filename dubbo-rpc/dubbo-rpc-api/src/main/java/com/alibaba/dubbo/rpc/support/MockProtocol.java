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
package com.alibaba.dubbo.rpc.support;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.Exporter;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.protocol.AbstractProtocol;

/**
 * MockProtocol is used for generating a mock invoker by URL and type on consumer side
 * 实现 AbstractProtocol 抽象类，用于在服务消费者，通过类型为 "mock" 的 URL ，引用创建 MockInvoker 对象
 */
final public class MockProtocol extends AbstractProtocol {

    @Override
    public int getDefaultPort() {
        return 0;
    }
    /**
     *  实现方法，不允许调用，直接抛出 UnsupportedOperationException 异常
     * @return
     */
    @Override
    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
        throw new UnsupportedOperationException();
    }

    /**
     * 引用创建 MockInvoker 对象。一般情况下，我们可以通过 dubbo-admin 运维平台或者直接向 Zookeeper 写入静态 URL,例如：
     *
     * 实际写入的 URL
     *  /dubbo/com.alibaba.dubbo.demo.DemoService/providers/mock%3A%2F%2F10.20.153.11%2Fcom.alibaba.dubbo.demo.DemoService%3Fdynamic%3Dtrue%26application%3Dfoo
     *
     *  decode URL
     *  /dubbo/com.alibaba.dubbo.demo.DemoService/providers/mock://10.20.153.11/com.alibaba.dubbo.demo.DemoService?dynamic=true&application=foo
     *
     * @param type Service class
     * @param url  URL address for the remote service
     * @param <T>
     * @return
     * @throws RpcException
     */
    @Override
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        return new MockInvoker<T>(url);
    }
}
