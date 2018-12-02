package com.teach.business;


import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * 注册序列化对象，否则会报错
 */
public class MyKryoRegistor implements KryoRegistrator {
    public void registerClasses(Kryo kryo) {
        kryo.register(org.apache.hadoop.hdfs.DistributedFileSystem.class);
    }
}
