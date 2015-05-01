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
package com.linkedin.cubert.io.rubix;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.data.Tuple;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.io.BlockInputStream;
import com.linkedin.cubert.io.CompactDeserializer;
import com.linkedin.cubert.io.SerializerUtils;
import com.linkedin.cubert.utils.print;

public class RubixRecordReader<K, V> extends RecordReader<K, V>
{
    private BlockInputStream blockInputStream;
    private InputStream in;
    private K key;
    private long length;
    private long bytesRead = 0;
    private long offset = 0;

    private Deserializer<V> valueDeserializer;

    private V value = null;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException
    {
        initialize(split, context.getConfiguration());
    }

    public void initialize(InputSplit split, Configuration conf) throws IOException,
            InterruptedException
    {
        @SuppressWarnings("unchecked")
        RubixInputSplit<K, V> rsplit = (RubixInputSplit<K, V>) split;

        SerializationFactory serializationFactory = new SerializationFactory(conf);
        switch (rsplit.getBlockSerializationType())
        {
        case DEFAULT:
            valueDeserializer =
                    serializationFactory.getDeserializer(rsplit.getValueClass());
            break;
        case COMPACT:
            BlockSchema schema = rsplit.getSchema();
            valueDeserializer = new CompactDeserializer<V>(schema);
            break;
        }

        key = rsplit.getKey();

        // store the blockid and partition key in the conf
        conf.setLong("MY_BLOCK_ID", rsplit.getBlockId());
        conf.setLong("MY_NUM_RECORDS", rsplit.getNumRecords());
        ByteArrayOutputStream tmpOut = new ByteArrayOutputStream();
        ((Tuple) key).write(new DataOutputStream(tmpOut));
        String keySerialized = SerializerUtils.serializeToString(tmpOut.toByteArray());
        conf.set("MY_PARTITION_KEY", keySerialized);

        Path path = rsplit.getFilename();
        offset = rsplit.getOffset();
        length = rsplit.getLength();

        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream fsin = fs.open(path);
        fsin.seek(offset);

        blockInputStream = new BlockInputStream(fsin, length);
        in = blockInputStream;

        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(path);
        if (codec != null)
        {
            print.f("codec is not null and it is %s", codec.getClass().toString());
            in = codec.createInputStream(in);
        }
        else
        {
            print.f("codec is null");
        }

        valueDeserializer.open(in);
    }

    @Override
    public boolean nextKeyValue() throws IOException
    {
        try
        {
            value = valueDeserializer.deserialize(value);
        }
        catch (IOException e)
        {
            return false;
        }

        return true;
    }

    @Override
    public K getCurrentKey() throws IOException,
            InterruptedException
    {
        K currentKey = key;

        // after returning the key, set it to null
        key = null;

        return currentKey;
    }

    @Override
    public V getCurrentValue() throws IOException
    {

        return value;
    }

    @Override
    public float getProgress() throws IOException,
            InterruptedException
    {
        bytesRead = blockInputStream.getBytesRead();
        return (float) (1.0 * bytesRead / length);
    }

    @Override
    public void close() throws IOException
    {
        in.close();
    }
}
