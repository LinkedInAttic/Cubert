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

import java.util.Iterator;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.utils.DataGenerator;
import com.linkedin.cubert.utils.TupleStore;

public class TestColumnarTupleStore
{
    final DataGenerator dataGenerator = new DataGenerator();

    @Test
    public void testAddAndIterate() throws Exception
    {
        final int nTests = 1000;
        final BlockSchema schema = DataGenerator.createBlockSchema();
        List<Tuple> tuples = dataGenerator.generateRandomTuples(nTests, schema);
        TupleStore store = new ColumnarTupleStore(schema);
        for (final Tuple t : tuples)
        {
            store.addToStore(t);
        }
        Iterator<Tuple> sit = store.iterator();
        for (Tuple tuple : tuples)
        {
            Assert.assertTrue(sit.hasNext());
            Assert.assertEquals(sit.next(), tuple);
        }
        Assert.assertFalse(sit.hasNext());
    }

    @Test
    public void testRandomAccess() throws Exception
    {
        final BlockSchema schema = DataGenerator.createBlockSchema();
        List<Tuple> tuples = dataGenerator.generateRandomTuples(1000, schema);
        TupleStore store = new ColumnarTupleStore(schema);
        for (final Tuple t : tuples)
        {
            store.addToStore(t);
        }
        int[] offsets = store.getOffsets();

        for (int i = 0; i < offsets.length; i++)
        {
            final Tuple t = store.getTuple(offsets[i], null);
            Assert.assertEquals(tuples.get(i), t);
        }
    }
}