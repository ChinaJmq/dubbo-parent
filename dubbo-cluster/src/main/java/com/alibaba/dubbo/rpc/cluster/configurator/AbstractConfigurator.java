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
package com.alibaba.dubbo.rpc.cluster.configurator;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.rpc.cluster.Configurator;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * AbstractOverrideConfigurator
 *
 */
public abstract class AbstractConfigurator implements Configurator {
    /**
     * 配置规则 URL
     */
    private final URL configuratorUrl;

    public AbstractConfigurator(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("configurator url == null");
        }
        this.configuratorUrl = url;
    }

    public static void main(String[] args) {
        System.out.println(URL.encode("timeout=100"));
    }

    @Override
    public URL getUrl() {
        return configuratorUrl;
    }

    /**
     * 一共有三种情况的判断：
     【第一种】第 8 行： configuratorUrl 带有端口( port )，意图是匹配指定一个服务提供者，因此使用 url.host 属性。
     【第二种】第 19 行：url 的 side = consumer ，意图是匹配服务消费者，因此使用 NetUtils#getLocalHost() 属性。
     【第三种】第 22 行：url 的 side = provider ，意图是匹配全部服务提供者，因此使用 Constants.ANYHOST_VALUE = * 属性。🙂 也就是说，目前暂不支持指定机器服务提供者。
      调用 #configureIfMatch(host, url) 方法，配置到 url 中，若配置规则匹配。
     * @param url - old provider url.
     * @return
     */
    @Override
    public URL configure(URL url) {
        if (configuratorUrl == null || configuratorUrl.getHost() == null
                || url == null || url.getHost() == null) {
            return url;
        }
        // If override url has port, means it is a provider address.
        // We want to control a specific provider with this override url, it may take effect on the specific provider instance or on consumers holding this provider instance.
        // 配置规则，URL 带有端口( port )，意图是控制提供者机器。可以在提供端生效 也可以在消费端生效
        if (configuratorUrl.getPort() != 0) {
            if (url.getPort() == configuratorUrl.getPort()) {
                return configureIfMatch(url.getHost(), url);
            }
        // 配置规则，URL 没有端口，override 输入消费端地址 或者 0.0.0.0
        } else {// override url don't have a port, means the ip override url specify is a consumer address or 0.0.0.0
            // 1.If it is a consumer ip address, the intention is to control a specific consumer instance, it must takes effect at the consumer side, any provider received this override url should ignore;
            // 2.If the ip is 0.0.0.0, this override url can be used on consumer, and also can be used on provider
            // 1. 如果是消费端地址，则意图是控制消费者机器，必定在消费端生效，提供端忽略；
            // 2. 如果是0.0.0.0可能是控制消费端，也可能是控制提供端
            if (url.getParameter(Constants.SIDE_KEY, Constants.PROVIDER).equals(Constants.CONSUMER)) {
                // NetUtils.getLocalHost是消费端注册到zk的消费者地址
                return configureIfMatch(NetUtils.getLocalHost(), url);// NetUtils.getLocalHost is the ip address consumer registered to registry.
            } else if (url.getParameter(Constants.SIDE_KEY, Constants.CONSUMER).equals(Constants.PROVIDER)) {
                // 控制所有提供端，地址必定是0.0.0.0，否则就要配端口从而执行上面的if分支了
                return configureIfMatch(Constants.ANYHOST_VALUE, url);// take effect on all providers, so address must be 0.0.0.0, otherwise it won't flow to this if branch
            }
        }
        return url;
    }

    private URL configureIfMatch(String host, URL url) {
        // 匹配 Host 第一种：0.0.0.0匹配 configuratorUrl.getHost() 第二种 特定的host匹配 configuratorUrl.getHost()
        if (Constants.ANYHOST_VALUE.equals(configuratorUrl.getHost()) || host.equals(configuratorUrl.getHost())) {
            //配置的application
            String configApplication = configuratorUrl.getParameter(Constants.APPLICATION_KEY,
                    configuratorUrl.getUsername());// TODO 为啥 username
            //当前的application
            String currentApplication = url.getParameter(Constants.APPLICATION_KEY, url.getUsername());
            // 匹配 "application"
            if (configApplication == null || Constants.ANY_VALUE.equals(configApplication)
                    || configApplication.equals(currentApplication)) {
                // 配置 URL 中的条件 KEYS 集合。其中下面四个 KEY ，不算是条件，而是内置属性。考虑到下面要移除，所以添加到该集合中。
                Set<String> condtionKeys = new HashSet<String>();
                //category=configurators 表示该数据为动态配置类型，必填。
                condtionKeys.add(Constants.CATEGORY_KEY);
                condtionKeys.add(Constants.CHECK_KEY);
                //dynamic=false 表示该数据为持久数据，当注册方退出时，数据依然保存在注册中心，必填。
                condtionKeys.add(Constants.DYNAMIC_KEY);
                //enabled=true 覆盖规则是否生效，可不填，缺省生效。
                condtionKeys.add(Constants.ENABLED_KEY);
                for (Map.Entry<String, String> entry : configuratorUrl.getParameters().entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    //判断传入的 url 是否匹配配置规则 URL 的条件。除了 "application" 和 "side" 之外，带有 "~" 开头的 KEY ，也是条件
                    if (key.startsWith("~") || Constants.APPLICATION_KEY.equals(key) || Constants.SIDE_KEY.equals(key)) {
                        condtionKeys.add(key);
                        // 若不相等，则不匹配配置规则，直接返回
                        if (value != null && !Constants.ANY_VALUE.equals(value)
                                && !value.equals(url.getParameter(key.startsWith("~") ? key.substring(1) : key))) {
                            return url;
                        }
                    }
                }
                // 移除条件 KEYS 集合，并配置到 URL 中
                return doConfigure(url, configuratorUrl.removeParameters(condtionKeys));
            }
        }
        return url;
    }

    /**
     * Sort by host, priority
     * 1. the url with a specific host ip should have higher priority than 0.0.0.0
     * 2. if two url has the same host, compare by priority value；
     *
     * 优先，按照 host 升序，即特定 host 高于 anyhost( "0.0.0.0" ) 。
       其次，按照 "priority" 降序。
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(Configurator o) {
        if (o == null) {
            return -1;
        }

        int ipCompare = getUrl().getHost().compareTo(o.getUrl().getHost());
        if (ipCompare == 0) {//host is the same, sort by priority
            int i = getUrl().getParameter(Constants.PRIORITY_KEY, 0),
                    j = o.getUrl().getParameter(Constants.PRIORITY_KEY, 0);
            if (i < j) {
                return -1;
            } else if (i > j) {
                return 1;
            } else {
                return 0;
            }
        } else {
            return ipCompare;
        }


    }

    protected abstract URL doConfigure(URL currentUrl, URL configUrl);

}
