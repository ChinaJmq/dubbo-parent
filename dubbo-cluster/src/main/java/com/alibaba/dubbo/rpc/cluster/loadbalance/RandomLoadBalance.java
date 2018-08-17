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
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;

import java.util.List;
import java.util.Random;

/**
 * random load balance.
 * 随机，按权重设置随机概率
 * 在一个截面上碰撞的概率高，但调用量越大分布越均匀，而且按概率使用权重后也比较均匀，有利于动态调整提供者权重
 *
 */
public class RandomLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "random";

    private final Random random = new Random();

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        //提供者数量
        int length = invokers.size(); // Number of invokers
        //总的权重数
        int totalWeight = 0; // The sum of weights
        // 各个Invoke权重是否都一样
        boolean sameWeight = true; // Every invoker has the same weight?
        // 计算总权限
        for (int i = 0; i < length; i++) {
            // 获得权重
            int weight = getWeight(invokers.get(i), invocation);
            // 累加总权重
            totalWeight += weight; // Sum
            // 每个invoke与前一次遍历的invoke的权重进行比较，判断所有权重是否一样
            if (sameWeight && i > 0
                    && weight != getWeight(invokers.get(i - 1), invocation)) {
                sameWeight = false;
            }
        }
        // 权重不相等，随机后，判断在哪个 Invoker 的权重区间中
        if (totalWeight > 0 && !sameWeight) {
            // If (not every invoker has the same weight & at least one invoker's weight>0), select randomly based on totalWeight.
            // 随机
            int offset = random.nextInt(totalWeight);
            // Return a invoker based on the random value.
            // 确定随机值落在哪个片断上，从而取得对应的invoke，如果weight越大，那么随机值落在其上的概率越大，这个invoke被选中的概率越大
            /**
             * 假定有3台dubbo provider:
             10.0.0.1:20884, weight=2
             10.0.0.1:20886, weight=3
             10.0.0.1:20888, weight=4
             随机算法的实现：
             totalWeight=9;
             假设offset=1（即random.nextInt(9)=1）
             1-2=-1<0？是，所以选中 10.0.0.1:20884, weight=2
             假设offset=4（即random.nextInt(9)=4）
             4-2=2<0？否，这时候offset=2， 2-3<0？是，所以选中 10.0.0.1:20886, weight=3
             假设offset=7（即random.nextInt(9)=7）
             7-2=5<0？否，这时候offset=5， 5-3=2<0？否，这时候offset=2， 2-4<0？是，所以选中 10.0.0.1:20888, weight=4
             */
            for (int i = 0; i < length; i++) {
                offset -= getWeight(invokers.get(i), invocation);
                if (offset < 0) {
                    return invokers.get(i);
                }
            }
        }
        // If all invokers have the same weight value or totalWeight=0, return evenly.
        // 权重相等，平均随机
        return invokers.get(random.nextInt(length));
    }

}
