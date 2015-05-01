/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.cubert.utils;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Data Generator for running unit tests
 *
 * Created by spyne on 10/15/14.
 */
public class DataGenerator
{
    final static Random random = new Random(12345L);

    private int MIN_STRING_LENGTH = 20;
    private int MAX_STRING_LENGTH = 100;
    private int MIN_INT = 0;
    private int MAX_INT = Integer.MAX_VALUE;

    private static final char[] SYMBOLS;

    static
    {
        StringBuilder tmp = new StringBuilder();
        for (char ch = '0'; ch <= '9'; ++ch) tmp.append(ch);
        for (char ch = 'a'; ch <= 'z'; ++ch) tmp.append(ch);
        for (char ch = 'A'; ch <= 'Z'; ++ch) tmp.append(ch);
        tmp.append(" -_{}[]()");
        SYMBOLS = tmp.toString().toCharArray();
    }

    private Object randomData(DataType type)
    {
        switch (type)
        {
            case INT: return MIN_INT + random.nextInt(MAX_INT - MIN_INT);
            case LONG: return random.nextLong();
            case DOUBLE: return random.nextDouble() * random.nextInt();
            case STRING: return nextString();
        }
        return null;
    }

    public int[] randomInts(int size)
    {
        int[] ints = new int[size];
        for (int i = 0; i < size; ++i)
        {
            ints[i] = (Integer) randomData(DataType.INT);
        }
        return ints;
    }

    public long[] randomLongs(int size)
    {
        long[] longs = new long[size];
        for (int i = 0; i < size; ++i)
        {
            longs[i] = (Long) randomData(DataType.LONG);
        }
        return longs;
    }

    public double[] randomDoubles(int size)
    {
        double[] doubles = new double[size];
        for (int i = 0; i < size; ++i)
        {
            doubles[i] = (Double) randomData(DataType.DOUBLE);
        }
        return doubles;
    }

    public String[] randomStrings(int size)
    {
        String[] strings = new String[size];
        for (int i = 0; i < size; ++i)
        {
            strings[i] = (String) randomData(DataType.STRING);
        }
        return strings;
    }

    public String nextString()
    {
        final int length = MIN_STRING_LENGTH + random.nextInt(MAX_STRING_LENGTH - MIN_STRING_LENGTH);
        final char[] buf = new char[length];
        for (int idx = 0; idx < buf.length; ++idx)
            buf[idx] = SYMBOLS[random.nextInt(SYMBOLS.length)];
        return new String(buf);
    }

    private Tuple createRandomTuple(final BlockSchema schema) throws ExecException
    {
        final int size = schema.getColumnTypes().length;
        final Tuple tuple = newTuple(size);
        for (int i = 0; i < size; ++i)
        {
            tuple.set(i, randomData(schema.getColumnType(i).getType()));
        }
        return tuple;
    }

    private Tuple createSequentialTuple(final BlockSchema schema, final Tuple prev) throws ExecException
    {
        final int size = schema.getColumnTypes().length;
        final Tuple tuple = newTuple(size);
        for (int i = 0; i < size; ++i)
        {
            tuple.set(i, nextInSequence(prev.get(i)));
        }
        return tuple;
    }

    public static Tuple newTuple(int size)
    {
        return TupleFactory.getInstance().newTuple(size);
    }

    private Object nextInSequence(Object o)
    {
        if (o instanceof Integer)
        {
            return (Integer) o + 1;
        }
        else if (o instanceof Long)
        {
            return (Long) o + 1;
        }
        else if (o instanceof Double)
        {
            return (Double) o + 1.0;
        }
        else if (o instanceof String)
        {
            /* TODO: Sequencial Not Implemented */
            return nextString();
        }
        return null;
    }

    public List<Tuple> generateSequentialTuples(final int N, final BlockSchema schema) throws ExecException
    {
        List<Tuple> tuples = new ArrayList<Tuple>();

        Tuple prev = newTuple(schema.getNumColumns());
        prev.set(0, 1);
        prev.set(1, 1L);
        prev.set(2, 1.0);
        prev.set(3, nextString());

        for (int i = 1; i <= N; i++)
        {
            Tuple t = createSequentialTuple(schema, prev);
            tuples.add(t);
            prev = t;
        }
        return tuples;
    }


    public List<Tuple> generateRandomTuples(final int N, final BlockSchema schema) throws ExecException
    {
        List<Tuple> tuples = new ArrayList<Tuple>();

        for (int i = 1; i <= N; i++)
        {
            Tuple t = createRandomTuple(schema);
            tuples.add(t);
        }
        return tuples;
    }

    public static BlockSchema createBlockSchema()
    {
        return new BlockSchema(new ColumnType[]{
                new ColumnType("Integer", DataType.INT),
                new ColumnType("Long", DataType.LONG),
                new ColumnType("Double", DataType.DOUBLE),
                new ColumnType("String", DataType.STRING),
        });
    }

    public void setMIN_STRING_LENGTH(int MIN_STRING_LENGTH)
    {
        this.MIN_STRING_LENGTH = MIN_STRING_LENGTH;
    }

    public void setMAX_STRING_LENGTH(int MAX_STRING_LENGTH)
    {
        this.MAX_STRING_LENGTH = MAX_STRING_LENGTH;
    }

    public void setMIN_INT(int MIN_INT)
    {
        this.MIN_INT = MIN_INT;
    }

    public void setMAX_INT(int MAX_INT)
    {
        this.MAX_INT = MAX_INT;
    }
}
