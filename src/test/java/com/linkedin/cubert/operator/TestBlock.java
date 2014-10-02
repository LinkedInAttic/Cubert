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

package com.linkedin.cubert.operator;

import java.io.IOException;
import java.util.Arrays;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.PivotedBlock;

public class TestBlock
{
    @BeforeClass
    public void setUp() throws JsonGenerationException,
            JsonMappingException,
            IOException
    {

    }

    void assertBlockData(Block block, int[] expected) throws IOException,
            InterruptedException
    {
        for (int e : expected)
        {

            Assert.assertEquals(block.next().get(0), e);
        }

        Assert.assertNull(block.next());
    }

    @Test
    public void testReadEmptyBlock() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows = {};
        Block block = new ArrayBlock(Arrays.asList(rows), new String[] { "A" });

        assert block.next() == null;
    }

    @Test
    public void testSimpleRead() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows = { { 1 }, { 2 }, { 3 } };
        Block block = new ArrayBlock(Arrays.asList(rows), new String[] { "a" });

        long count = 1;
        Tuple tuple;
        while ((tuple = block.next()) != null)
        {
            int val = (Integer) tuple.get(0);
            assert val == count;
            count++;
        }

        Assert.assertEquals(count, 4);

    }

    @Test
    public void testPivotedRead() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows = { { 1, 10 }, { 1, 20 }, { 2, 20 } };
        String[] colNames = { "a", "b" };
        Block block = new ArrayBlock(Arrays.asList(rows), colNames);
        Tuple tuple;

        PivotedBlock pblock = new PivotedBlock(block, new String[] { "a" });

        tuple = pblock.next();
        assertTuple(colNames, new int[] { 1, 10 }, tuple);

        tuple = pblock.next();
        assertTuple(colNames, new int[] { 1, 20 }, tuple);

        tuple = pblock.next();
        Assert.assertNull(tuple);

        boolean more = pblock.advancePivot();
        Assert.assertTrue(more);
        tuple = pblock.next();
        assertTuple(colNames, new int[] { 2, 20 }, tuple);

        tuple = pblock.next();
        Assert.assertNull(tuple);

        more = pblock.advancePivot();
        Assert.assertFalse(more);
    }

    void assertTuple(String[] colNames, int[] expected, Tuple tuple) throws ExecException
    {
        int ncols = colNames.length;
        Assert.assertNotNull(tuple);
        for (int col = 0; col < ncols; col++)
        {
            int found = (Integer) tuple.get(col);
            Assert.assertEquals(found, expected[col]);
        }
    }
}
