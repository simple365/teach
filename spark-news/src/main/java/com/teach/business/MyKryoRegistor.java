package com.teach.business;


import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

public class MyKryoRegistor implements KryoRegistrator {
    public void registerClasses(Kryo kryo) {
        kryo.register(org.apache.hadoop.hdfs.DistributedFileSystem.class);
    }
}
