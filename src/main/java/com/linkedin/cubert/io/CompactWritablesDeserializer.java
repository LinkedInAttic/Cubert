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

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;

public class CompactWritablesDeserializer<K> implements Deserializer<K>
{
    private InputStream in;
    private final DataType[] datatypes;
    private final Writable[] writables;
    private byte[] buffer = new byte[32];

    public CompactWritablesDeserializer(BlockSchema schema)
    {
        if (!schema.isFlatSchema())
            throw new IllegalArgumentException("CompactWritablesDeserializer can be used with flat tuple schema only");

        datatypes = new DataType[schema.getNumColumns()];
        writables = new Writable[schema.getNumColumns()];

        for (int i = 0; i < datatypes.length; i++)
        {
            ColumnType ctype = schema.getColumnType(i);
            datatypes[i] = ctype.getType();
            writables[i] = createWritable(datatypes[i]);
        }
    }

    @Override
    public void open(InputStream in) throws IOException
    {
        this.in = in;
    }

    private static final WritableComparable<?> createWritable(DataType type)
    {
        switch (type)
        {
        case BOOLEAN:
            return new BooleanWritable();
        case BYTE:
            return new ByteWritable();
        case INT:
            return new IntWritable();
        case LONG:
            return new LongWritable();
        case FLOAT:
            return new FloatWritable();
        case DOUBLE:
            return new DoubleWritable();
        case STRING:
            return new Text();
        default:
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public K deserialize(K object) throws IOException
    {
        if (in.available() == 0)
            throw new IOException();

        Tuple t = (Tuple) object;

        if (t == null)
        {
            t = TupleFactory.getInstance().newTuple(datatypes.length);
        }

        for (int i = 0; i < datatypes.length; i++)
        {
            Object field = null;

            switch (datatypes[i])
            {
            case BOOLEAN:
            {
                IntWritable iw = VariableLengthEncoder.decodeInteger(in);
                if (iw != null)
                {
                    ((BooleanWritable) writables[i]).set(iw.get() == 1);
                    field = writables[i];
                }
                break;
            }
            case BYTE:
            {
                IntWritable iw = VariableLengthEncoder.decodeInteger(in);
                if (iw != null)
                {
                    ((ByteWritable) writables[i]).set((byte) iw.get());
                    field = writables[i];
                }
                break;
            }
            case DOUBLE:
            {
                DoubleWritable dw = VariableLengthEncoder.decodeDouble(in);
                if (dw != null)
                {
                    ((DoubleWritable) writables[i]).set(dw.get());
                    field = writables[i];
                }
                break;
            }
            case FLOAT:
            {
                FloatWritable fw = VariableLengthEncoder.decodeFloat(in);
                if (fw != null)
                {
                    ((FloatWritable) writables[i]).set(fw.get());
                    field = writables[i];
                }
                break;
            }
            case INT:
            {
                IntWritable iw = VariableLengthEncoder.decodeInteger(in);
                if (iw != null)
                {
                    ((IntWritable) writables[i]).set(iw.get());
                    field = writables[i];
                }
                break;
            }
            case LONG:
            {
                LongWritable lw = VariableLengthEncoder.decodeLong(in);
                if (lw != null)
                {
                    ((LongWritable) writables[i]).set(lw.get());
                    field = writables[i];
                }
                break;
            }
            case STRING:
            {
                IntWritable iw = VariableLengthEncoder.decodeInteger(in);
                if (iw != null)
                {
                    int length = iw.get();

                    if (length > buffer.length)
                        buffer = new byte[2 * buffer.length];

                    in.read(buffer, 0, length);
                    ((Text) writables[i]).set(new String(buffer, 0, length));
                    field = writables[i];
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
