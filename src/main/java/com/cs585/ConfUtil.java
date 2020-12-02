package com.cs585;

import org.apache.hadoop.conf.Configuration;

public class ConfUtil {

    /**
     * Configuration设置自定义类数据
     *
     * @param key              变量名
     * @param conf             Configuration
     * @param userDefineObject 自定义类
     */
    public static void setClass(String key, Configuration conf, Object userDefineObject) {

        String userStr = com.alibaba.fastjson.JSON.toJSON(userDefineObject).toString();
        conf.set(key, userStr);
    }

    /**
     * Configuration 获得自定义数据类
     *
     * @param key       变量名
     * @param conf      Configuration
     * @param classType 返回值类型
     * @return
     */
    public static Object getClass(String key, Configuration conf, Class<?> classType) {
        String str = conf.get(key);
        Object object = com.alibaba.fastjson.JSON.parseObject(str, classType);
        return object;
    }
}
