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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;

/**
 * Deserializes tuples stored using variable length encoding.
 * 
 * @author Maneesh Varshney
 * 
 * @param <K>
 */
public class CompactDeserializer<K> implements Deserializer<K>
{

    private InputStream in;
    private DataType[] datatypes;
    private byte[] buffer = new byte[32];

    public CompactDeserializer(BlockSchema schema)
    {
        if (!schema.isFlatSchema())
            throw new IllegalArgumentException("CompactDeserializer can be used with flat tuple schema only");

        datatypes = new DataType[schema.getNumColumns()];
        for (int i = 0; i < datatypes.length; i++)
        {
            ColumnType ctype = schema.getColumnType(i);
            datatypes[i] = ctype.getType();
        }
    }

    @Override
    public void open(InputStream in) throws IOException
    {
        this.in = in;
    }

    @SuppressWarnings("unchecked")
    @Override
    public K deserialize(K object) throws IOException
    {
        if (in.available() == 0)
            throw new EOFException();

        Tuple t = (Tuple) object;

        if (t == null || t.size() != datatypes.length)
            t = TupleFactory.getInstance().newTuple(datatypes.length);

        for (int i = 0; i < datatypes.length; i++)
        {
            Object field = null;

            switch (datatypes[i])
            {
            case BOOLEAN:
            {
                IntWritable writable = VariableLengthEncoder.decodeInteger(in);
                field = (writable == null) ? null : (Boolean) (writable.get() == 1);
                break;
            }
            case BYTE:
            {
                IntWritable writable = VariableLengthEncoder.decodeInteger(in);
                field = (writable == null) ? null : (byte) writable.get();
                break;
            }
            case DOUBLE:
            {
                DoubleWritable writable = VariableLengthEncoder.decodeDouble(in);
                field = (writable == null) ? null : writable.get();
                break;
            }
            case FLOAT:
            {
                FloatWritable writable = VariableLengthEncoder.decodeFloat(in);
                field = (writable == null) ? null : writable.get();
                break;
            }
            case INT:
            {
                IntWritable writable = VariableLengthEncoder.decodeInteger(in);
                field = (writable == null) ? null : writable.get();
                break;
            }
            case LONG:
            {
                LongWritable writable = VariableLengthEncoder.decodeLong(in);
                field = (writable == null) ? null : writable.get();
                break;
            }
            case STRING:
            {
                IntWritable writable = VariableLengthEncoder.decodeInteger(in);
                if (writable == null)
                    field = null;
                else
                {
                    int length = writable.get();
                    if (length > buffer.length)
                        buffer = new byte[2 * buffer.length];

                    in.read(buffer, 0, length);
                    field = new String(buffer, 0, length);
                }
                break;
            }
            default:
                throw new RuntimeException("Cannot deserialize column of type "
                        + datatypes[i]);
            }

            t.set(i, field);
        }

        return (K) t;
    }

    @Override
    public void close() throws IOException
    {
        // TODO Auto-generated method stub

    }

}
