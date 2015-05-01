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

package com.linkedin.cubert.memory;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.cubert.utils.DataGenerator;

import java.util.Iterator;

/**
 * Unit Test class for IntArrayList, LongArrayList, DoubleArrayList, SegmentedArrayList
 *
 * Created by spyne on 1/8/15.
 *
 */
public class TestSegmentedArrayLists
{
    @Test
    public void testIntArrayListAddAndGet() throws Exception
    {
        IntArrayList list = new IntArrayList(101);

        DataGenerator dgen = new DataGenerator();
        final int size = 1000;
        final int[] ints = dgen.randomInts(size);

        for (int i = 0; i < size; ++i)
        {
            list.addInt(ints[i]);
        }

        Assert.assertEquals(size, list.size());
        for (int i = 0; i < size; ++i)
        {
            Assert.assertEquals(ints[i], list.get(i));
        }

        for (int i = 0; i < size - 1; ++i)
        {
            final Integer act1 = ints[i], act2 = ints[i+1];
            Assert.assertEquals(act1.compareTo(act2), list.compareIndices(i, i + 1));
        }
    }

    @Test
    public void testLongArrayListAddAndGet() throws Exception
    {
        LongArrayList list = new LongArrayList(101);

        DataGenerator dgen = new DataGenerator();
        final int size = 1000;
        final long[] longs = dgen.randomLongs(size);

        for (int i = 0; i < size; ++i)
        {
            list.addLong(longs[i]);
        }

        Assert.assertEquals(size, list.size());
        for (int i = 0; i < size; ++i)
        {
            Assert.assertEquals(longs[i], list.get(i));
        }

        for (int i = 0; i < size - 1; ++i)
        {
            final Long act1 = longs[i], act2 = longs[i+1];
            Assert.assertEquals(act1.compareTo(act2), list.compareIndices(i, i + 1));
        }
    }

    @Test
    public void testDoubleArrayListAddAndGet() throws Exception
    {
        DoubleArrayList list = new DoubleArrayList(101);

        DataGenerator dgen = new DataGenerator();
        final int size = 1000;
        final double[] doubles = dgen.randomDoubles(size);

        for (int i = 0; i < size; ++i)
        {
            list.add(doubles[i]);
        }

        Assert.assertEquals(size, list.size());
        for (int i = 0; i < size; ++i)
        {
            Assert.assertEquals(doubles[i], list.get(i));
        }

        for (int i = 0; i < size - 1; ++i)
        {
            final Double act1 = doubles[i], act2 = doubles[i+1];
            Assert.assertEquals(act1.compareTo(act2), list.compareIndices(i, i + 1));
        }
    }

    @Test
    public void testSegmentedArrayListAddAndGet() throws Exception
    {
        ObjectArrayList list = new ObjectArrayList(101);

        DataGenerator dgen = new DataGenerator();
        final int size = 1000;
        final String[] strings = dgen.randomStrings(size);

        for (int i = 0; i < size; ++i)
        {
            list.add(strings[i]);
        }

        Assert.assertEquals(size, list.size());
        for (int i = 0; i < size; ++i)
        {
            Assert.assertEquals(strings[i], list.get(i));
        }

        for (int i = 0; i < size - 1; ++i)
        {
            final String act1 = strings[i], act2 = strings[i+1];
            Assert.assertEquals(act1.compareTo(act2), list.compareIndices(i, i + 1));
        }
    }

    @Test
    public void testBagArrayList() throws Exception
    {
        SegmentedArrayList array = new BagArrayList(new BlockSchema("INT a, DOUBLE b, STRING c"), false);

        final int N = 10000;
        DataBag[] bags = new DataBag[N];
        int counter = 0;

        for (int i = 0; i < N; i++)
        {
            Tuple[] tuplesInBag = new Tuple[(i % 5) + 1];

            for (int j = 0; j < tuplesInBag.length; j++)
            {
                tuplesInBag[j] = createTuple(counter, counter * 1.0, Integer.toString(counter));
                counter++;
            }

            bags[i] = createBag(tuplesInBag);
        }

        for (DataBag bag: bags)
            array.add(bag);

        Assert.assertEquals(array.size, N);

        for (int i = 0; i < bags.length; i++)
        {
            assertBagEqual((DataBag) array.get(i), bags[i]);
        }
    }

    @Test
    public void testNestedSchema() throws Exception
    {
        ColumnType tupleFieldType =
                new ColumnType("element", DataType.TUPLE, new BlockSchema("STRING name, STRING term, FLOAT value"));
        BlockSchema tupleSchema = new BlockSchema(new ColumnType[] { tupleFieldType });

        ColumnType bagType = new ColumnType("bag", DataType.BAG, tupleSchema);
        BlockSchema schema = new BlockSchema(new ColumnType[]
                                                     {
                                                             new ColumnType("member_id", DataType.INT),
                                                             bagType
                                                     });

        final int N = 10000;
        int counter = 1;
        Tuple[] data = new Tuple[N];
        for (int i = 0; i < N; i++)
        {
            Tuple[] tuplesInBag = new Tuple[(i % 5) + 1];

            for (int j = 0; j < tuplesInBag.length; j++)
            {
                tuplesInBag[j] = createTuple("name " + counter,
                                             "term " + counter,
                                             (counter % 3 == 0) ? null : counter * 1.0f);
                counter++;
            }

            DataBag bag = createBag(tuplesInBag);

            data[i] = createTuple(i, bag);
        }


        ColumnarTupleStore store = new ColumnarTupleStore(schema, true);
        for (Tuple t: data)
            store.addToStore(t);

        Assert.assertEquals(store.getNumTuples(), N);

        for (int i = 0; i < N; i++)
        {
            Tuple actual = store.getTuple(i, null);
            Tuple expected = data[i];
            Assert.assertEquals(actual.get(0), expected.get(0));
            assertBagEqual((DataBag) actual.get(1), (DataBag) expected.get(1));
        }
    }

    private Tuple createTuple(Object... args) throws ExecException
    {
        Tuple tuple = TupleFactory.getInstance().newTuple(args.length);
        for (int i = 0; i < args.length; i++)
        {
            tuple.set(i, args[i]);
        }

        return tuple;
    }

    private DataBag createBag(Tuple... tuples)
    {
        DataBag bag = BagFactory.getInstance().newDefaultBag();
        for (Tuple tuple: tuples)
            bag.add(tuple);

        return bag;
    }

    private void assertBagEqual(DataBag bag1, DataBag bag2)
    {
        Iterator<Tuple> it1 = bag1.iterator();
        Iterator<Tuple> it2 = bag2.iterator();
        while (it1.hasNext())
        {
            Assert.assertTrue(it2.hasNext());
            Tuple tuple1 = it1.next();
            Tuple tuple2 = it2.next();
            Assert.assertEquals(tuple1, tuple2, tuple1.toString() + " != " + tuple2.toString());
        }

        Assert.assertFalse(it2.hasNext());
    }

}
