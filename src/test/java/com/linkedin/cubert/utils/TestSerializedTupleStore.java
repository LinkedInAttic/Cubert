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

package com.linkedin.cubert.utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.pig.data.Tuple;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.operator.PhaseContext;
import com.linkedin.cubert.plan.physical.CubertStrings;
import org.junit.Test;
import org.testng.Assert;

/**
 * Unit tests for SerializedTupleStore
 *
 * Created by spyne on 7/30/14.
 */
public class TestSerializedTupleStore
{
    final DataGenerator dataGenerator = new DataGenerator();

    public static void setup(Boolean useCompactSerialization) throws IOException
    {
        final Configuration conf = new Configuration();
        conf.set(CubertStrings.USE_COMPACT_SERIALIZATION, useCompactSerialization.toString());
        PhaseContext.create((Mapper.Context) null, conf);
    }

    @Test
    public void testAddAndIterateUsingCompactSerialization() throws Exception
    {
        setup(true);

        final int nTests = 1000;
        performTests(nTests);
    }

    @Test
    public void testAddAndIterate() throws Exception
    {
        setup(false);

        final int nTests = 1000;
        performTests(nTests);
    }

    private void performTests(int nTests) throws IOException
    {
        final BlockSchema schema = DataGenerator.createBlockSchema();
        List<Tuple> tuples = dataGenerator.generateRandomTuples(nTests, schema);
        SerializedTupleStore store = new SerializedTupleStore(schema);
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
        SerializedTupleStore store = new SerializedTupleStore(schema, new String[]{"Integer"});
        for (final Tuple t : tuples)
        {
            store.addToStore(t);
        }
        List<Integer> offsets = store.getStartOffsetList();

        for (int i = 0; i < offsets.size(); i++)
        {
            Integer offset = offsets.get(i);
            final Tuple t = store.getTuple(offset, null);
            Assert.assertEquals(tuples.get(i), t);
        }
    }
}
