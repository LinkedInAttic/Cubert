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

import java.io.IOException;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.utils.DataGenerator;
import com.linkedin.cubert.utils.SerializedTupleStore;
import com.linkedin.cubert.utils.TestSerializedTupleStore;
import com.linkedin.cubert.utils.TupleStore;
import com.linkedin.cubert.utils.print;

/**
 * Unit tests for LookUpTable
 *
 * Created by spyne on 10/15/14.
 */
public class TestLookUpTable
{
    private static final int N_TUPLES = 1000;

    private final TupleStore store;
    private final String[] comparatorKeys = {"Integer"};
    private final int[] comparatorIndices;

    public TestLookUpTable() throws IOException
    {
        TestSerializedTupleStore.setup(false);
        final BlockSchema schema = DataGenerator.createBlockSchema();
        DataGenerator instance = new DataGenerator();
        List<Tuple> tuples = instance.generateRandomTuples(N_TUPLES, schema);

        store = new SerializedTupleStore(schema, comparatorKeys);
        for (final Tuple t : tuples)
        {
            store.addToStore(t);
        }

        comparatorIndices = new int[comparatorKeys.length];
        for (int i = 0; i < comparatorIndices.length; ++i)
        {
            comparatorIndices[i] = schema.getIndex(comparatorKeys[i]);
        }
    }

    @Test
    public void testHashTable() throws Exception
    {
        LookUpTable lookUpTable = new LookUpTable(store, comparatorKeys);

        for(int i = 0; i < lookUpTable.size() - 1; ++i)
        {
            Assert.assertTrue(lookUpTable.getSortable().compare(i, i + 1) <= 0);
        }

        long start, end, total = 0;
        final int[] offsets = store.getOffsets();
        for (int offset : offsets)
        {
            Tuple t = store.getTuple(offset, null);

            start = System.currentTimeMillis();
            Tuple key = getKeyTuple(t);
            final List<Tuple> tuples = lookUpTable.get(key);
            end = System.currentTimeMillis();
            total += end - start;

            Assert.assertNotNull(tuples);
            Assert.assertTrue(tuples.contains(t));
            for (Tuple result : tuples)
            {
                for (int i = 0; i < comparatorIndices.length; ++i)
                {
                    Assert.assertEquals(t.get(i), result.get(i));
                }
            }
        }
        print.f("TestLookUpTable: HashTable queries for %d entries done in %d ms", lookUpTable.size(), total);
    }

    private Tuple getKeyTuple(Tuple t) throws ExecException
    {
        Tuple key = TupleFactory.getInstance().newTuple(comparatorIndices.length);
        for (int i = 0; i < comparatorIndices.length; i++)
        {
            key.set(i, t.get(comparatorIndices[i]));
        }
        return key;
    }

    public static void main(String[] args) throws Exception
    {

    }
}
