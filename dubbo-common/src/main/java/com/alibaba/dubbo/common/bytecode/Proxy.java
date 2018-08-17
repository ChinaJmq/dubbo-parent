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
package com.alibaba.dubbo.common.bytecode;

import com.alibaba.dubbo.common.utils.ClassHelper;
import com.alibaba.dubbo.common.utils.ReflectUtils;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Proxy.
 */

public abstract class Proxy {
    public static final InvocationHandler RETURN_NULL_INVOKER = new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            return null;
        }
    };
    public static final InvocationHandler THROW_UNSUPPORTED_INVOKER = new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            throw new UnsupportedOperationException("Method [" + ReflectUtils.getName(method) + "] unimplemented.");
        }
    };
    private static final AtomicLong PROXY_CLASS_COUNTER = new AtomicLong(0);
    private static final String PACKAGE_NAME = Proxy.class.getPackage().getName();
    private static final Map<ClassLoader, Map<String, Object>> ProxyCacheMap = new WeakHashMap<ClassLoader, Map<String, Object>>();

    private static final Object PendingGenerationMarker = new Object();

    protected Proxy() {
    }

    /**
     * Get proxy.
     *
     * @param ics interface class array.
     * @return Proxy instance.
     */
    public static Proxy getProxy(Class<?>... ics) {
        return getProxy(ClassHelper.getClassLoader(Proxy.class), ics);
    }

    /**
     * Get proxy.
     *
     * @param cl  class loader.
     * @param ics interface class array.
     * @return Proxy instance.
     */
    public static Proxy getProxy(ClassLoader cl, Class<?>... ics) {
        // 校验接口超过上限
        if (ics.length > 65535)
            throw new IllegalArgumentException("interface limit exceeded");

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ics.length; i++) {
            String itf = ics[i].getName();
            // 校验是否为接口
            if (!ics[i].isInterface())
                throw new RuntimeException(itf + " is not a interface.");
            // 加载接口类
            Class<?> tmp = null;
            try {
                tmp = Class.forName(itf, false, cl);
            } catch (ClassNotFoundException e) {
            }

            if (tmp != ics[i])
                throw new IllegalArgumentException(ics[i] + " is not visible from class loader");

            sb.append(itf).append(';');
        }

        // use interface class name list as key.
        String key = sb.toString();

        // get cache by class loader.
        Map<String, Object> cache;
        /**
         * 此处必须处理多线程问题，如果线程1执行完ProxyCacheMap.put(cl, cache),
         * 线程2判断cache == null而cpu正好切换停留在此处，线程1继续执行，这时cache正好存入数据，
         * 然后线程2继续执行，会新创建HashMap对象，会覆盖掉原有的
         * 值得注意的是ProxyCacheMap是WeakHashMap,适用于缓存，当key没有强引用时，gc回收时会回收次键值对
         * WeakHashMap具体参见
         * http://www.cnblogs.com/skywang12345/p/3311092.html
         * https://blog.csdn.net/yangzl2008/article/details/6980709
         */
        synchronized (ProxyCacheMap) {
            cache = ProxyCacheMap.get(cl);
            if (cache == null) {
                cache = new HashMap<String, Object>();
                ProxyCacheMap.put(cl, cache);
            }
        }
        // 获得 Proxy 工厂
        /**
         * 首先这段代码需要细细的品味
         * 1.用do...while循环来获取proxy，先从缓存中获取，如果获取到Reference<?>，从Reference<?>中取proxy，然后直接return
         * 2.没有取到Reference<?>，先判断value是否为PendingGenerationMarker，如果为，说明在proxy创建中，其他创建请求等待，避免并发，
         *   直到创建成功才会唤醒，继续do...while循环，下次循环将会获取到Reference<?>
         * 3.如果value不为PendingGenerationMarker，将PendingGenerationMarker放到缓存中,然后break挑出do...while循环去创建proxy，如果proxy在创建中，后续的线程获取到的都是PendingGenerationMarker
         * 又说明做线程cache.put(key, PendingGenerationMarker)操作的一定是第一个线程
         *
         */
        Proxy proxy = null;
        synchronized (cache) {
            do {
                // 从缓存中获取 Proxy 工厂
                Object value = cache.get(key);
                //proxy用弱引用存储，具体参见4中引用https://www.cnblogs.com/xdouby/p/6793184.html
                if (value instanceof Reference<?>) {
                    proxy = (Proxy) ((Reference<?>) value).get();
                    if (proxy != null)
                        return proxy;
                }
                // 缓存中不存在，设置生成 Proxy 代码标记。创建中时，其他创建请求等待，避免并发。
                if (value == PendingGenerationMarker) {
                    try {
                        cache.wait();
                    } catch (InterruptedException e) {
                    }
                } else {
                    cache.put(key, PendingGenerationMarker);
                    break;
                }
            }
            while (true);
        }
        //为生成类名唯一的标识
        long id = PROXY_CLASS_COUNTER.getAndIncrement();
        String pkg = null;
        // ccp->proxy class generator   ccm->proxy factory class generator
        ClassGenerator ccp = null, ccm = null;
        try {
            // 创建 class 代码生成器
            ccp = ClassGenerator.newInstance(cl);

            Set<String> worked = new HashSet<String>();
            List<Method> methods = new ArrayList<Method>();

            for (int i = 0; i < ics.length; i++) {
                // 非 public 接口，使用接口包名
                if (!Modifier.isPublic(ics[i].getModifiers())) {
                    String npkg = ics[i].getPackage().getName();
                    if (pkg == null) {
                        pkg = npkg;
                    } else {
                        if (!pkg.equals(npkg))// 实现了两个非 public 的接口
                            throw new IllegalArgumentException("non-public interfaces from different packages");
                    }
                }
                // 添加接口
                ccp.addInterface(ics[i]);
                //遍历所有的public方法
                for (Method method : ics[i].getMethods()) {
                    // 方法的具体描述，添加方法签名到已处理方法签名集合
                    //添加方法签名到已处理方法签名集合。多个接口可能存在相同的接口方法，跳过相同的方法，避免冲突
                    String desc = ReflectUtils.getDesc(method);
                    if (worked.contains(desc))
                        continue;
                    worked.add(desc);

                    int ix = methods.size();
                    //方法返回值类型
                    Class<?> rt = method.getReturnType();
                    //方法参数类型
                    Class<?>[] pts = method.getParameterTypes();
                    //生成方法体
                    StringBuilder code = new StringBuilder("Object[] args = new Object[").append(pts.length).append("];");
                    for (int j = 0; j < pts.length; j++)
                        code.append(" args[").append(j).append("] = ($w)$").append(j + 1).append(";");
                    code.append(" Object ret = handler.invoke(this, methods[" + ix + "], args);");
                    if (!Void.TYPE.equals(rt))
                        code.append(" return ").append(asArgument(rt, "ret")).append(";");
                    // 添加方法
                    methods.add(method);
                    ccp.addMethod(method.getName(), method.getModifiers(), rt, pts, method.getExceptionTypes(), code.toString());
                }
            }
            // 设置包路径
            if (pkg == null)
                pkg = PACKAGE_NAME;

            // create ProxyInstance class.

            String pcn = pkg + ".proxy" + id;
            // 设置类名
            ccp.setClassName(pcn);
            // 添加静态属性 methods
            ccp.addField("public static java.lang.reflect.Method[] methods;");
            // 添加属性 handler
            ccp.addField("private " + InvocationHandler.class.getName() + " handler;");
            // 添加构造方法，参数 handler
            ccp.addConstructor(Modifier.PUBLIC, new Class<?>[]{InvocationHandler.class}, new Class<?>[0], "handler=$1;");
            // 添加构造方法，参数 空
            ccp.addDefaultConstructor();
            //生成代理proxy
            Class<?> clazz = ccp.toClass();
            // 生成类
            clazz.getField("methods").set(null, methods.toArray(new Method[0]));

            // create Proxy class.
            String fcn = Proxy.class.getName() + id;
            // 创建  class 代码生成器
            ccm = ClassGenerator.newInstance(cl);
            // 设置类名
            ccm.setClassName(fcn);
            // 添加构造方法，参数 空
            ccm.addDefaultConstructor();
            // 设置父类为 TccProxy.class
            ccm.setSuperClass(Proxy.class);
            // 添加方法 #newInstance(handler)
            ccm.addMethod("public Object newInstance(" + InvocationHandler.class.getName() + " h){ return new " + pcn + "($1); }");
            // 生成类
            Class<?> pc = ccm.toClass();
            // 创建 Proxy 对象
            proxy = (Proxy) pc.newInstance();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            // release ClassGenerator
            if (ccp != null)
                ccp.release();
            if (ccm != null)
                ccm.release();
            synchronized (cache) {
                if (proxy == null)
                    cache.remove(key);
                else
                    cache.put(key, new WeakReference<Proxy>(proxy));
                cache.notifyAll();
            }
        }
        return proxy;
    }

    private static String asArgument(Class<?> cl, String name) {
        if (cl.isPrimitive()) {
            if (Boolean.TYPE == cl)
                return name + "==null?false:((Boolean)" + name + ").booleanValue()";
            if (Byte.TYPE == cl)
                return name + "==null?(byte)0:((Byte)" + name + ").byteValue()";
            if (Character.TYPE == cl)
                return name + "==null?(char)0:((Character)" + name + ").charValue()";
            if (Double.TYPE == cl)
                return name + "==null?(double)0:((Double)" + name + ").doubleValue()";
            if (Float.TYPE == cl)
                return name + "==null?(float)0:((Float)" + name + ").floatValue()";
            if (Integer.TYPE == cl)
                return name + "==null?(int)0:((Integer)" + name + ").intValue()";
            if (Long.TYPE == cl)
                return name + "==null?(long)0:((Long)" + name + ").longValue()";
            if (Short.TYPE == cl)
                return name + "==null?(short)0:((Short)" + name + ").shortValue()";
            throw new RuntimeException(name + " is unknown primitive type.");
        }
        return "(" + ReflectUtils.getName(cl) + ")" + name;
    }

    /**
     * get instance with default handler.
     *
     * @return instance.
     */
    public Object newInstance() {
        return newInstance(THROW_UNSUPPORTED_INVOKER);
    }

    /**
     * get instance with special handler.
     *
     * @return instance.
     */
    abstract public Object newInstance(InvocationHandler handler);
}
