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
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.CreateBlockOperator;

public class TestBlockCreatorOperator
{

    void assertCreatedBlock(Object[][] rows,
                            String[] colNames,
                            int numPartitionKeys,
                            String blockgenType,
                            int blockgenValue,
                            int[] expectedCounts) throws JsonParseException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        String[] aa = Arrays.copyOf(colNames, numPartitionKeys);
        String[] bb = new String[aa.length];
        for (int i = 0; i < aa.length; i++)
        {
            bb[i] = String.format("'%s'", aa[i]);
        }
        String partitionKeysStr = Arrays.toString(bb);

        Block block = new ArrayBlock(Arrays.asList(rows), colNames);

        ObjectMapper mapper = new ObjectMapper();
        String jsonString =
                String.format("{'operator': 'CREATE_BLOCK', 'blockgenType': '%s',"
                        + "'blockgenValue': '%d', 'partitionKeys': %s}",

                blockgenType, blockgenValue, partitionKeysStr);

        JsonNode json = mapper.readValue(jsonString.replace("'", "\""), JsonNode.class);
        CreateBlockOperator operator = new CreateBlockOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block);

        operator.setInput(null, input, json);

        int blocks = 0;
        Block b;
        while ((b = operator.next()) != null)
        {

            int nrows = 0;
            while (b.next() != null)
            {
                nrows++;
            }
            Assert.assertEquals(expectedCounts[blocks], nrows);
            blocks++;
        }

        Assert.assertEquals(expectedCounts.length, blocks);
    }

    @Test
    public void testByRow() throws JsonParseException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows = { { 3, 1 }, { 3, 2 }, { 10, 1 } };
        int[] expected = { 2, 1 };
        assertCreatedBlock(rows, new String[] { "a", "b" }, 1, "BY_ROW", 1, expected);

        assertCreatedBlock(rows, new String[] { "a", "b" }, 1, "BY_ROW", 2, expected);

        expected = new int[] { 2, 1 };
        assertCreatedBlock(rows, new String[] { "a", "b" }, 1, "BY_ROW", 3, expected);
    }

}
