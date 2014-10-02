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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.io.CompactDeserializer;
import com.linkedin.cubert.io.CompactSerializer;
import com.linkedin.cubert.io.CompactWritablesDeserializer;

public class TestCompactSerialization
{

    private Tuple newNullTuple(int size)
    {
        return TupleFactory.getInstance().newTuple(size);
    }

    private Tuple newTuple(Object... args)
    {
        Tuple tuple = TupleFactory.getInstance().newTuple(args.length);
        for (int i = 0; i < args.length; i++)
        {
            try
            {
                tuple.set(i, args[i]);
            }
            catch (ExecException e)
            {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        return tuple;
    }

    private void validate(BlockSchema schema, List<Tuple> list)
    {
        try
        {
            validate2(schema, list);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void validate2(BlockSchema schema, List<Tuple> list) throws IOException
    {
        // serialize
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        CompactSerializer<Tuple> ser = new CompactSerializer<Tuple>(schema);

        ser.open(bos);

        for (Tuple tuple : list)
            ser.serialize(tuple);

        ser.close();

        // deserialize
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        CompactDeserializer<Tuple> des = new CompactDeserializer<Tuple>(schema);

        des.open(bis);

        Tuple tuple = null;
        for (int i = 0; i < list.size(); i++)
        {
            tuple = des.deserialize(tuple);
            Assert.assertEquals(tuple.toString(), list.get(i).toString());
        }

        des.close();

        // writables deserializer
        bis = new ByteArrayInputStream(bos.toByteArray());
        CompactWritablesDeserializer<Tuple> des2 =
                new CompactWritablesDeserializer<Tuple>(schema);

        des2.open(bis);

        tuple = null;
        for (int i = 0; i < list.size(); i++)
        {
            tuple = des2.deserialize(tuple);
            Assert.assertEquals(tuple.toString(), list.get(i).toString());
        }

        des2.close();

    }

    @Test
    public void testIntegers()
    {
        BlockSchema schema = new BlockSchema("int col");

        List<Tuple> list = new ArrayList<Tuple>();

        // all bit lengths
        for (int i = 0; i <= 32; i++)
        {
            int num = 0;
            for (int j = 0; j < i; j++)
                num |= 1 << j;

            list.add(newTuple(num));
            list.add(newTuple(-num));
        }

        // special values
        list.add(newTuple(Integer.MAX_VALUE));
        list.add(newTuple(Integer.MIN_VALUE));

        // random values
        Random rand = new Random(1);
        for (int i = 0; i < 1000; i++)
            list.add(newTuple(rand.nextInt()));

        // null value
        list.add(newNullTuple(1));

        validate(schema, list);
    }

    @Test
    public void testLongs()
    {
        BlockSchema schema = new BlockSchema("long col");

        List<Tuple> list = new ArrayList<Tuple>();

        // all bit lengths
        for (int i = 0; i <= 64; i++)
        {
            long num = 0;
            for (int j = 0; j < i; j++)
                num |= 1L << j;

            list.add(newTuple(num));
            list.add(newTuple(-num));
        }

        // special values
        list.add(newTuple(Long.MAX_VALUE));
        list.add(newTuple(Long.MIN_VALUE));

        // random values
        Random rand = new Random(1);
        for (int i = 0; i < 1000; i++)
            list.add(newTuple(rand.nextLong()));

        // null value
        list.add(newNullTuple(1));

        validate(schema, list);
    }

    @Test
    public void testFloats()
    {
        BlockSchema schema = new BlockSchema("float col");

        List<Tuple> list = new ArrayList<Tuple>();

        // all bit lengths
        for (int i = 0; i <= 32; i++)
        {
            int num = 0;
            for (int j = 0; j < i; j++)
                num |= 1 << j;

            float f = (float) num;
            list.add(newTuple(f));
            list.add(newTuple(-f));
        }

        // special values
        list.add(newTuple(0.0f));
        list.add(newTuple(-0.0f));
        list.add(newTuple(Float.MIN_VALUE));
        list.add(newTuple(Float.MAX_VALUE));
        list.add(newTuple(Float.POSITIVE_INFINITY));
        list.add(newTuple(Float.NEGATIVE_INFINITY));

        // random float values
        Random rand = new Random(1);
        for (int i = 0; i < 1000; i++)
            list.add(newTuple(rand.nextFloat()));

        // random integers as floats
        for (int i = 0; i < 1000; i++)
            list.add(newTuple((float) rand.nextInt()));

        // null value
        list.add(newNullTuple(1));

        validate(schema, list);

    }

    @Test
    public void testDoubles()
    {
        BlockSchema schema = new BlockSchema("double col");

        List<Tuple> list = new ArrayList<Tuple>();

        // all bit lengths
        for (int i = 0; i <= 64; i++)
        {
            long num = 0;
            for (int j = 0; j < i; j++)
                num |= 1L << j;

            double d = (double) num;
            list.add(newTuple(d));
            list.add(newTuple(-d));
        }

        // special values
        list.add(newTuple(0.0d));
        list.add(newTuple(-0.0d));
        list.add(newTuple(Double.MIN_VALUE));
        list.add(newTuple(Double.MAX_VALUE));
        list.add(newTuple(Double.POSITIVE_INFINITY));
        list.add(newTuple(Double.NEGATIVE_INFINITY));

        // random float values
        Random rand = new Random(1);
        for (int i = 0; i < 1000; i++)
            list.add(newTuple(rand.nextDouble()));

        // random longs as floats
        for (int i = 0; i < 1000; i++)
            list.add(newTuple((double) rand.nextLong()));

        // null value
        list.add(newNullTuple(1));

        validate(schema, list);
    }

    @Test
    public void testBooleans()
    {
        BlockSchema schema = new BlockSchema("boolean col");

        List<Tuple> list = new ArrayList<Tuple>();

        list.add(newTuple(true));
        list.add(newTuple(false));
        list.add(newNullTuple(1));

        validate(schema, list);
    }

    @Test
    public void testStrings()
    {
        BlockSchema schema = new BlockSchema("string col");

        List<Tuple> list = new ArrayList<Tuple>();
        list.add(newTuple("test"));
        list.add(newTuple(""));
        list.add(newNullTuple(1));

        validate(schema, list);
    }

    @Test
    public void testByte()
    {
        BlockSchema schema = new BlockSchema("byte col");
        List<Tuple> list = new ArrayList<Tuple>();

        for (int i = 0; i < 256; i++)
            list.add(newTuple((byte) i));
        list.add(newNullTuple(1));

        validate(schema, list);

    }

    @Test
    public void testMultiTypes()
    {
        BlockSchema schema =
                new BlockSchema("int col1, long col2, float col3, double col4, boolean col5, byte col6, string col7");
        List<Tuple> list = new ArrayList<Tuple>();
        Random rand = new Random(1);

        for (int i = 0; i < 1; i++)
        {
            list.add(newTuple(rand.nextInt(),
                              rand.nextLong(),
                              rand.nextFloat(),
                              rand.nextDouble(),
                              rand.nextBoolean(),
                              (byte) rand.nextInt(256),
                              "str" + rand.nextInt()));
        }

        validate(schema, list);
    }

    @Test
    public void testMultiTypesWithNull()
    {
        BlockSchema schema =
                new BlockSchema("int col1, long col2, float col3, double col4, boolean col5, byte col6, string col7");
        List<Tuple> list = new ArrayList<Tuple>();
        Random rand = new Random(1);

        for (int i = 0; i < 1000; i++)
        {
            list.add(newTuple(i % 5 == 0 ? null : rand.nextInt(),
                              i % 6 == 0 ? null : rand.nextLong(),
                              i % 7 == 0 ? null : rand.nextFloat(),
                              i % 8 == 0 ? null : rand.nextDouble(),
                              i % 9 == 0 ? null : rand.nextBoolean(),
                              i % 4 == 0 ? null : (byte) rand.nextInt(256),
                              i % 10 == 0 ? null : "str" + rand.nextInt()));
        }

        validate(schema, list);
    }
}
