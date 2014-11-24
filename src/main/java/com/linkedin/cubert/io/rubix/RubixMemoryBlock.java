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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.io.BlockSerializationType;
import com.linkedin.cubert.io.CompactDeserializer;

public class RubixMemoryBlock implements Block
{
    private final BlockProperties props;
    private RubixTupleCreator tupleCreator;
    private ByteBuffer byteBuffer;
    private final BlockSerializationType serializationType;

    private ByteBufferBackedInputStream inputStream;
    private Deserializer<Tuple> deserializer;
    private Tuple value;

    private int mark = 0;
    private int currentTuplePosition = 0;

    public RubixMemoryBlock(BlockProperties props,
                            Configuration conf,
                            ByteBuffer byteBuffer,
                            Class<Tuple> valueClass,
                            CompressionCodec codec,
                            BlockSerializationType serializationType) throws IOException
    {
        this.props = props;
        this.serializationType = serializationType;
        this.byteBuffer = byteBuffer;
        this.tupleCreator = new RubixTupleCreator();

        this.inputStream = new ByteBufferBackedInputStream(byteBuffer);

        switch (serializationType)
        {
        case DEFAULT:
            SerializationFactory serializationFactory = new SerializationFactory(conf);
            deserializer = serializationFactory.getDeserializer(valueClass);
            break;
        case COMPACT:
            deserializer = new CompactDeserializer<Tuple>(props.getSchema());
            break;

        }

        if (codec == null)
        {
            deserializer.open(inputStream);
        }
        else
        {
            deserializer.open(codec.createInputStream(inputStream));
        }
        this.mark = this.currentTuplePosition = 0;
    }

    @Override
    public BlockProperties getProperties()
    {
        return props;
    }

    public Tuple next() throws IOException
    {
        currentTuplePosition = byteBuffer.position();
        try
        {
            value = deserializer.deserialize(value);
        }
        catch (EOFException e)
        {
            return null;
        }

        if (value == null)
            return null;

        return tupleCreator.create(null, value);
    }

    @Override
    public void configure(JsonNode json) throws IOException
    {
        tupleCreator.setup(json);
    }

    public BlockSerializationType getSerializationType()
    {
        return serializationType;
    }

    @Override
    public void rewind() throws IOException
    {
        this.seek(mark);
    }

    public void markRewindPosition()
    {
        mark = currentTuplePosition;

    }

    class ByteBufferBackedInputStream extends InputStream
    {

        ByteBuffer buf;

        public ByteBufferBackedInputStream(ByteBuffer buf)
        {
            this.buf = buf;
        }

        public int read() throws IOException
        {
            if (!buf.hasRemaining())
            {
                return -1;
            }
            return buf.get() & 0xFF;
        }

        public int read(byte[] bytes, int off, int len) throws IOException
        {
            if (!buf.hasRemaining())
            {
                return -1;
            }

            len = Math.min(len, buf.remaining());
            buf.get(bytes, off, len);
            return len;
        }

        @Override
        public int available() throws IOException
        {
            return buf.remaining();
        }

    }

    public int getCurrentTuplePosition()
    {
        return this.currentTuplePosition;
    }

    public void seek(int pos)
    {
        this.byteBuffer.position(pos);
    }

    public ByteBuffer getByteBuffer()
    {
        return byteBuffer;
    }
}
