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

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.plan.physical.TestContext;
import com.linkedin.cubert.utils.JsonUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/***
 * Tests for OLAP cube additive.
 * 
 * tests sum aggregate on a single group by and also on grouping sets,
 * 
 * @author Krishna Puttaswamy
 * 
 */

public class TestOLAPCube
{
    @BeforeClass
    public void setUp() throws JsonGenerationException,
            JsonMappingException,
            IOException
    {

    }

    void validate(Object[][] rows, String[] expected) throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        validateGroupingSets(rows, expected, null);
    }

    void validateGroupingSets(Object[][] rows, String[] expected, String[] groupingSets) throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        /* Step 1: Create input block schema */
        int ndims = rows[0].length - 1;
        String[] dimensions = new String[ndims];
        String[] columnNames = new String[ndims + 1];
        columnNames[0] = "clickCount";
        StringBuffer typeName = new StringBuffer();
        for (int i = 0; i < ndims; i++)
        {
            if (i > 0)
                typeName.append(",");
            typeName.append("int ");
            String name = "Dim" + i;
            typeName.append(name);
            columnNames[i + 1] = name;
            dimensions[i] = name;
        }
        BlockSchema inputSchema = new BlockSchema(typeName.toString());

        /** Step 2: Create json */
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();

        Configuration conf = new JobConf();
        PhaseContext.create((MapContext) new TestContext(), conf);
        PhaseContext.create((ReduceContext) new TestContext(), conf);

        // add aggregates into json
        ArrayNode measures = mapper.createArrayNode();
        measures.add(JsonUtils.createObjectNode("type",
                                                "SUM",
                                                "input",
                                                "clickCount",
                                                "output",
                                                "sum_clicks"));
        node.put("aggregates", measures);

        // add dimensions into json
        ArrayNode dimensionNode = mapper.createArrayNode();
        for (int i = 0; i < dimensions.length; i++)
            dimensionNode.add(dimensions[i]);
        node.put("dimensions", dimensionNode);

        // add grouping sets into json
        ArrayNode groupingSetNode = mapper.createArrayNode();
        if (groupingSets != null)
            for (String str : groupingSets)
                groupingSetNode.add(str);
        node.put("groupingSets", groupingSetNode);

        /** Step 3: create the input block */
        HashMap<String, Block> map = new HashMap<String, Block>();
        Block block = new ArrayBlock(Arrays.asList(rows), columnNames, 1);
        map.put("block", block);

        /** Step 4: create CUBE operator, initialize */
        CubeOperator cd = new CubeOperator();

        BlockSchema outputSchema = inputSchema.append(new BlockSchema("INT sum_clicks"));
        BlockProperties props =
                new BlockProperties(null, outputSchema, (BlockProperties) null);
        cd.setInput(map, node, props);

        /** Step 5: get the results from CUBE operator and put them in a set */
        Set<String> computed = new HashSet<String>();
        Tuple tuple;

        while ((tuple = cd.next()) != null)
        {
            computed.add(tuple.toString());
        }

        /** Step 6: validate that computed and brute force results are same */
        // System.out.println("Aggregated:" + computed);
        // System.out.println("Expected: " + java.util.Arrays.toString(expected));
        Assert.assertEquals(computed.size(), expected.length);

        for (String entry : expected)
        {
            Assert.assertTrue(computed.contains(entry));
        }
    }

    @Test
    void testNoOverlap() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows =
                { { 1, (int) 10 }, { 1, (int) 20 }, { 2, (int) 30 }, { 2, (int) 40 },
                        { 3, (int) 50 } };

        String[] expected =
                new String[] { "(10,1)", "(20,1)", "(30,2)", "(40,2)", "(50,3)", "(,9)" };

        validate(rows, expected);
    }

    @Test
    void testTotalClickCount() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        // clickCount
        // dimensions: country code, number of monitors, vegetarian
        Object[][] rows =
                { { 1, (int) 1, (int) 1, (int) 1 }, { 1, (int) 1, (int) 1, (int) 2 },
                        { 2, (int) 1, (int) 2, (int) 1 },
                        { 3, (int) 1, (int) 2, (int) 2 },
                        { 2, (int) 2, (int) 2, (int) 2 } };

        String[] expected =
                new String[] { "(1,,,7)", "(2,,,2)", "(,1,,2)", "(,2,,7)", "(,,1,3)",
                        "(,,2,6)",

                        "(1,1,,2)", "(1,2,,5)", "(2,2,,2)",

                        "(,1,1,1)", "(,1,2,1)", "(,2,1,2)", "(,2,2,5)",

                        "(1,,1,3)", "(1,,2,4)", "(2,,2,2)",

                        "(1,1,1,1)", "(1,1,2,1)", "(1,2,1,2)", "(1,2,2,3)", "(2,2,2,2)",

                        "(,,,9)" };

        validate(rows, expected);
    }

    @Test
    void testGroupingSetsSum() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        // clickCount
        // dimensions: country code, number of monitors, vegetarian
        Object[][] rows =
                { { 1, (int) 1, (int) 1, (int) 1 }, { 1, (int) 1, (int) 1, (int) 2 },
                        { 2, (int) 1, (int) 2, (int) 1 },
                        { 3, (int) 1, (int) 2, (int) 2 },
                        { 2, (int) 2, (int) 2, (int) 2 } };

        String[] expected =
                new String[] { "(1,,,7)", "(2,,,2)", "(1,1,,2)", "(1,2,,5)", "(2,2,,2)" };

        validateGroupingSets(rows, expected, new String[] { "Dim0,Dim1", "Dim0" });
    }
}
