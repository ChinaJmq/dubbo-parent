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

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.AtomicPositiveInteger;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Round robin load balance.
 * 轮循，按公约后的权重设置轮循比率
 * 存在慢的提供者累积请求的问题，比如：第二台机器很慢，但没挂，当请求调到第二台时就卡在那，久而久之，所有请求都卡在调到第二台上。
 *
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "roundrobin";
    /**
     6:      * 服务方法与计数器的映射
     7:      *
     8:      * KEY：serviceKey + "." + methodName
     9:      */
    private final ConcurrentMap<String, AtomicPositiveInteger> sequences = new ConcurrentHashMap<String, AtomicPositiveInteger>();

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // key表示方法，例如com.alibaba.dubbo.demo.TestService.getRandomNumber， 即负载均衡算法细化到每个方法的调用；
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        // provider的总个数
        int length = invokers.size(); // Number of invokers
        // 最大权重, 只是一个临时值, 所以设置为0, 当遍历invokers时接口的weight肯定大于0，马上就会替换成一个真实的maxWeight的值；
        int maxWeight = 0; // The maximum weight
        // 最小权重，只是一个临时值, 所以设置为Integer类型最大值, 当遍历invokers时接口的weight肯定小于这个数，马上就会替换成一个真实的minWeight的值；
        int minWeight = Integer.MAX_VALUE; // The minimum weight
        //Invoker 与其权重的映射。其中，IntegerWrapper 为 RoundRobinLoadBalance 的内部类
        final LinkedHashMap<Invoker<T>, IntegerWrapper> invokerToWeightMap = new LinkedHashMap<Invoker<T>, IntegerWrapper>();
        //总的权重和
        int weightSum = 0;
        for (int i = 0; i < length; i++) {
            // 从Invoker的URL中获取权重时，dubbo会判断是否warnup了，即只有当invoke这个jvm进程的运行时间超过warnup(默认为10分钟)时间，配置的weight才会生效；
            int weight = getWeight(invokers.get(i), invocation);
            // 重新计算最大权重值
            maxWeight = Math.max(maxWeight, weight); // Choose the maximum weight
            // 重新计算最小权重值
            minWeight = Math.min(minWeight, weight); // Choose the minimum weight
            if (weight > 0) {
                invokerToWeightMap.put(invokers.get(i), new IntegerWrapper(weight));
                weightSum += weight;
            }
        }
        // 获得 AtomicPositiveInteger 对象
        AtomicPositiveInteger sequence = sequences.get(key);
        if (sequence == null) {
            sequences.putIfAbsent(key, new AtomicPositiveInteger());
            sequence = sequences.get(key);
        }
        // 获得当前顺序号，并递增 + 1,也就是请求数
        int currentSequence = sequence.getAndIncrement();
        // 权重不相等，顺序根据权重分配
        // 如果各provider配置的权重不一样
        if (maxWeight > 0 && minWeight < maxWeight) {
            // 剩余权重
            int mod = currentSequence % weightSum;
            // 循环最大权重
            for (int i = 0; i < maxWeight; i++) {
                // 循环 Invoker 集合
                for (Map.Entry<Invoker<T>, IntegerWrapper> each : invokerToWeightMap.entrySet()) {
                    final Invoker<T> k = each.getKey();
                    final IntegerWrapper v = each.getValue();
                    // 剩余权重归 0 ，当前 Invoker 还有剩余权重，返回该 Invoker 对象
                    if (mod == 0 && v.getValue() > 0) {
                        return k;
                    }
                    // 若 Invoker 还有权重值，扣除它( value )和剩余权重( mod )。
                    if (v.getValue() > 0) {
                        v.decrement();
                        mod--;
                    }
                }
            }
        }
        // Round robin
        return invokers.get(currentSequence % length);
    }

    private static final class IntegerWrapper {
        private int value;

        public IntegerWrapper(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public void decrement() {
            this.value--;
        }
    }

}
