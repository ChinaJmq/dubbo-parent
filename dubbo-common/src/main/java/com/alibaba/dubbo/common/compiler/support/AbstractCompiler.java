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
package com.alibaba.dubbo.common.compiler.support;

import com.alibaba.dubbo.common.compiler.Compiler;
import com.alibaba.dubbo.common.utils.ClassHelper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract compiler. (SPI, Prototype, ThreadSafe)
 */
public abstract class AbstractCompiler implements Compiler {

    private static final Pattern PACKAGE_PATTERN = Pattern.compile("package\\s+([$_a-zA-Z][$_a-zA-Z0-9\\.]*);");

    private static final Pattern CLASS_PATTERN = Pattern.compile("class\\s+([$_a-zA-Z][$_a-zA-Z0-9]*)\\s+");

    @Override
    public Class<?> compile(String code, ClassLoader classLoader) {
        code = code.trim();
        // 获得包名
        Matcher matcher = PACKAGE_PATTERN.matcher(code);
        String pkg;
        if (matcher.find()) {
            pkg = matcher.group(1);
        } else {
            pkg = "";
        }
        // 获得类名
        matcher = CLASS_PATTERN.matcher(code);
        String cls;
        if (matcher.find()) {
            cls = matcher.group(1);
        } else {
            throw new IllegalArgumentException("No such class name in " + code);
        }
        // 获得完整类名
        String className = pkg != null && pkg.length() > 0 ? pkg + "." + cls : cls;
        try {
            /**
             * getClass()为真正调用类的class，如用默认的JavassistCompiler编译，则getClass()就为JavassistCompiler的class
             *加载成功，说明已存在,类不存在，说明可能未编译过，进行编译
             */
            return Class.forName(className, true, ClassHelper.getCallerClassLoader(getClass()));
        } catch (ClassNotFoundException e) {
            // 代码格式不正确
            if (!code.endsWith("}")) {
                throw new IllegalStateException("The java code not endsWith \"}\", code: \n" + code + "\n");
            }
            try {
                return doCompile(className, code);
            } catch (RuntimeException t) {
                throw t;
            } catch (Throwable t) {
                throw new IllegalStateException("Failed to compile class, cause: " + t.getMessage() + ", class: " + className + ", code: \n" + code + "\n, stack: " + ClassUtils.toString(t));
            }
        }
    }

    /**
     * 编译代码
     *
     * @param name 类名
     * @param source 代码
     * @return 编译后的类
     * @throws Throwable 发生异常
     */
    protected abstract Class<?> doCompile(String name, String source) throws Throwable;

}
