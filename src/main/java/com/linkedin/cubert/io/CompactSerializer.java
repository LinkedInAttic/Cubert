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
import java.io.OutputStream;

import org.apache.hadoop.io.serializer.Serializer;
import org.apache.pig.data.Tuple;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.DataType;

/**
 * Serializes tuples using variable byte encoding.
 * 
 * @author Maneesh Varshney
 * 
 * @param <K>
 */
public class CompactSerializer<K> implements Serializer<K>
{
    private OutputStream out;
    private DataType[] datatypes;

    public CompactSerializer(BlockSchema schema)
    {
        if (!schema.isFlatSchema())
            throw new IllegalArgumentException("CompactSerializer can be used with flat tuple schema only");

        datatypes = new DataType[schema.getNumColumns()];
        for (int i = 0; i < schema.getNumColumns(); i++)
            datatypes[i] = schema.getColumnType(i).getType();
    }

    @Override
    public void close() throws IOException
    {

    }

    @Override
    public void open(OutputStream out) throws IOException
    {
        this.out = out;
    }

    @Override
    public void serialize(K object) throws IOException
    {
        Tuple tuple = (Tuple) object;

        for (int i = 0; i < datatypes.length; i++)
        {
            Object field = tuple.get(i);

            switch (datatypes[i])
            {
            case BOOLEAN:
                if (field == null)
                    VariableLengthEncoder.encodeNullInteger(out);
                else
                    VariableLengthEncoder.encodeInteger((Boolean) field ? 1 : 0, out);
                break;
            case BYTE:
                if (field == null)
                    VariableLengthEncoder.encodeNullInteger(out);
                else
                    VariableLengthEncoder.encodeInteger((Byte) field, out);
                break;
            case DOUBLE:
                if (field == null)
                    VariableLengthEncoder.encodeNullDouble(out);
                else
                    VariableLengthEncoder.encodeDouble(((Number) field).doubleValue(),
                                                       out);
                break;
            case FLOAT:
                if (field == null)
                    VariableLengthEncoder.encodeNullFloat(out);
                else
                    VariableLengthEncoder.encodeFloat(((Number) field).floatValue(), out);
                break;
            case INT:
                if (field == null)
                    VariableLengthEncoder.encodeNullInteger(out);
                else
                    VariableLengthEncoder.encodeInteger(((Number) field).intValue(), out);
                break;
            case LONG:
                if (field == null)
                    VariableLengthEncoder.encodeNullLong(out);
                else
                    VariableLengthEncoder.encodeLong(((Number) field).longValue(), out);
                break;
            case STRING:
                if (field == null)
                    VariableLengthEncoder.encodeNullInteger(out);
                else
                {
                    String s = (String) field;
                    byte[] bytes = s.getBytes();
                    VariableLengthEncoder.encodeInteger(bytes.length, out);
                    out.write(bytes);
                }
                break;
            default:
                throw new RuntimeException("Cannot serialize column of type "
                        + datatypes[i]);

            }
        }
    }

}
