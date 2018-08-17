package com.alibaba.dubbo.demo.provider;

import com.alibaba.dubbo.rpc.service.GenericException;
import com.alibaba.dubbo.rpc.service.GenericService;

/**
 * Created by 恒安 on 2018/7/17.
 *
 * @author mingqiang ji
 */
public class MyGenericService implements GenericService {
    @Override
    public Object $invoke(String methodName, String[] parameterTypes, Object[] args) throws GenericException {
        if ("sayHello".equals(methodName)) {
            return "Welcome " + args[0];
        }
        return "xxxx";
    }
}
