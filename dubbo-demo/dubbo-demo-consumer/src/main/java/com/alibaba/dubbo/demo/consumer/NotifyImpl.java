package com.alibaba.dubbo.demo.consumer;

import com.alibaba.dubbo.demo.Notify;

/**
 * Created by 恒安 on 2018/7/16.
 *
 * @author mingqiang ji
 */
public class NotifyImpl implements Notify {
    @Override
    public void oninvoke(String name) {
        System.out.println("oninvoke"+name);
    }

    @Override
    public void onreturn(String result,String name) {
        System.out.println(result);
        System.out.println(name);

    }

    @Override
    public void onthrow(String result,String name) {
        System.out.println(result);
        System.out.println(name);

    }
}
