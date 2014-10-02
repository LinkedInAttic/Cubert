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

package com.linkedin.cubert.block;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;

import com.linkedin.cubert.io.BlockSerializationType;
import com.linkedin.cubert.io.CompactDeserializer;

public class BlockIterator<V> implements Iterator<V>
{
    private final Deserializer<V> deserializer;
    private V value = null;

    public BlockIterator(Configuration conf,
                         InputStream inStream,
                         Class<V> valueClass,
                         BlockSerializationType serializationType,
                         BlockSchema schema) throws IOException,
            InstantiationException,
            IllegalAccessException
    {
        switch (serializationType)
        {
        case DEFAULT:
            SerializationFactory serializationFactory = new SerializationFactory(conf);
            deserializer = serializationFactory.getDeserializer(valueClass);
            break;
        case COMPACT:
            deserializer = new CompactDeserializer<V>(schema);
            break;
        default:
            deserializer = null;
        }

        deserializer.open(inStream);
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNext()
    {
        return false;
    }

    @Override
    public V next()
    {
        try
        {
            value = deserializer.deserialize(value);
        }
        catch (EOFException e)
        {
            value = null;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return value;
    }
}
