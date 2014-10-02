/* (c) 2014 LinkedIn Corp. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.cubert.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.xml.bind.DatatypeConverter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SerializerUtils
{
    public static byte[] serializeToBytes(Object object) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);

        oos.writeObject(object);
        oos.close();

        return bos.toByteArray();
    }

    public static Object deserializeFromByte(byte[] bytes) throws IOException,
            ClassNotFoundException
    {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);

        ObjectInputStream ois = new ObjectInputStream(bis);
        Object obj = ois.readObject();
        ois.close();

        return obj;
    }

    public static Object deserializeFromBytes(byte[] bytes) throws IOException,
            ClassNotFoundException
    {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);

        ObjectInputStream ois = new ObjectInputStream(bis);
        Object obj = ois.readObject();
        ois.close();

        return obj;
    }

    public static String serializeToString(Object object) throws IOException
    {
        byte[] bytes = serializeToBytes(object);

        return DatatypeConverter.printBase64Binary(bytes);
    }

    public static Object deserializeFromString(String str) throws IOException,
            ClassNotFoundException
    {
        byte[] bytes = DatatypeConverter.parseBase64Binary(str);
        return deserializeFromBytes(bytes);
    }

    public static void serializeToFile(Configuration conf, Path path, Object object) throws IOException
    {
        FSDataOutputStream out = FileSystem.get(conf).create(path);
        GZIPOutputStream gzip = new GZIPOutputStream(out);
        ObjectOutputStream oos = new ObjectOutputStream(gzip);

        oos.writeObject(object);
        oos.close();
    }

    public static Object deserializeFromFile(String path) throws FileNotFoundException,
            IOException,
            ClassNotFoundException
    {
        FileInputStream fis = new FileInputStream(path);
        GZIPInputStream gzip = new GZIPInputStream(fis);
        ObjectInputStream ois = new ObjectInputStream(gzip);
        Object object = ois.readObject();
        ois.close();

        return object;
    }
}
