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
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.operator.CubeOperator;
import com.linkedin.cubert.utils.JsonUtils;

/***
 * Tests for OLAP cube count distinct. main cases covered: olap cube with 1 dimension,
 * multiple dimension, full cube, grouping sets, with and without overlap in aggregate,
 * single and multiple measures, unit tests to test the dimension fields of type long.
 * 
 * @author Krishna Puttaswamy
 * 
 */

public class TestOLAPCubeCountDistinct
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
        /* Step 1: create input block schema */
        int ndims = rows[0].length - 1;
        String[] dimensions = new String[ndims];
        String[] columnNames = new String[ndims + 1];
        columnNames[0] = "member";
        StringBuffer typeName = new StringBuffer();
        for (int i = 0; i < ndims; i++)
        {
            if (i > 0)
                typeName.append(",");
            if (rows[0][i + 1] instanceof Integer)
                typeName.append("int ");
            else
                typeName.append("long ");

            String name = "Dim" + i;
            typeName.append(name);

            columnNames[i + 1] = name;
            dimensions[i] = name;
        }

        BlockSchema inputSchema = new BlockSchema(typeName.toString());

        /* Step 2: Create input block */
        Block block = new ArrayBlock(Arrays.asList(rows), columnNames, 1);
        HashMap<String, Block> map = new HashMap<String, Block>();
        map.put("block", block);

        // System.out.println("SCHEMA " + block.getSchema());

        /* Step 3: create json */
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();

        // add aggregates
        ArrayNode measures = mapper.createArrayNode();
        ObjectNode measureNode =
                JsonUtils.createObjectNode("input",
                                           "member",
                                           "output",
                                           "count_distinct_members",
                                           "type",
                                           "COUNT_DISTINCT");
        measures.add(measureNode);
        node.put("aggregates", measures);

        // add dimensions
        ArrayNode dimensionNode = mapper.createArrayNode();
        for (int i = 0; i < dimensions.length; i++)
        {
            dimensionNode.add(dimensions[i]);
        }
        node.put("dimensions", dimensionNode);

        // add innerDimensions
        node.put("innerDimensions", "member");

        // add grouping sets
        ArrayNode groupingSetNode = mapper.createArrayNode();
        if (groupingSets != null)
            for (String str : groupingSets)
                groupingSetNode.add(str);
        node.put("groupingSets", groupingSetNode);

        /* Step 4: create and initialize CUBE operator */
        CubeOperator cd = new CubeOperator();
        BlockSchema outputSchema =
                inputSchema.append(new BlockSchema("LONG count_distinct_members"));
        BlockProperties props =
                new BlockProperties(null, outputSchema, (BlockProperties) null);
        cd.setInput(map, node, props);

        // aggregate the output
        // Map<DimensionKey, int[]> aggregated = new HashMap<DimensionKey, int[]>();

        /* Step 5: store the output of CUBE operator in a set */
        Set<String> computed = new HashSet<String>();

        Tuple tuple;
        while ((tuple = cd.next()) != null)
        {
            computed.add(tuple.toString());
        }

        /* Step 6: validate the computed results against expected */
        if (expected.length > computed.size())
        {
            System.out.println("EXPECTED: " + java.util.Arrays.toString(expected));
            System.out.println("COMPUTED: " + computed);
            Set<String> a = new HashSet<String>();
            for (String s : expected)
                a.add(s);
            for (String s : computed)
                a.remove(s);
            System.out.println("Remaining: " + a);
        }

        Assert.assertEquals(expected.length, computed.size());

        for (String entry : expected)
        {
            if (!computed.contains(entry))
                Assert.assertFalse(true, entry);

            Assert.assertTrue(computed.contains(entry));
        }
    }

    @Test
    void testOneMember() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows = { { 1, (int) 10, (int) 100 }, { 1, (int) 10, (int) 200 } };
        String[] expected =
                new String[] { "(10,,1)", "(,100,1)", "(,200,1)", "(,,1)", "(10,100,1)",
                        "(10,200,1)" };
        validate(rows, expected);
    }

    @Test
    void testOneDimension() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows = { { 1, (int) 10 }, { 2, (int) 10 }, { 2, (int) 10 } };
        String[] expected = new String[] { "(10,2)", "(,2)" };
        validate(rows, expected);
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
                new String[] { "(10,1)", "(20,1)", "(30,1)", "(40,1)", "(50,1)", "(,3)" };
        validate(rows, expected);
    }

    @Test
    void testThreeDimsTeam() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows =
                { { 1, (int) 1, (int) 2, (int) 1 }, { 2, (int) 1, (int) 2, (int) 2 },
                        { 3, (int) 1, (int) 1, (int) 1 },
                        { 4, (int) 1, (int) 1, (int) 2 },
                        { 5, (int) 2, (int) 2, (int) 2 } };
        String[] expected =
                new String[] { "(1,2,1,1)", "(1,2,2,1)", "(1,1,1,1)", "(1,1,2,1)",
                        "(2,2,2,1)", "(1,,,4)", "(2,,,1)", "(,1,,2)", "(,2,,3)",
                        "(,,1,2)", "(,,2,3)", "(1,1,,2)", "(1,2,,2)", "(2,2,,1)",
                        "(,1,1,1)", "(,1,2,1)", "(,2,1,1)", "(,2,2,2)", "(1,,1,2)",
                        "(1,,2,2)", "(2,,2,1)", "(,,,5)" };
        validate(rows, expected);
    }

    @Test
    void testLongType() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        // members: srinivas, maneesh, krishna, saurabh, rui
        // dimensions: country code, number of monitors, vegetarian
        Object[][] rows =
                { { 1, (long) 1, (long) 2, (long) 1 },
                        { 2, (long) 1, (long) 2, (long) 2 },
                        { 3, (long) 1, (long) 1, (long) 1 },
                        { 4, (long) 1, (long) 1, (long) 2 },
                        { 5, (long) 2, (long) 2, (long) 2 } };
        String[] expected =
                new String[] { "(1,2,1,1)", "(1,2,2,1)", "(1,1,1,1)", "(1,1,2,1)",
                        "(2,2,2,1)", "(1,,,4)", "(2,,,1)", "(,1,,2)", "(,2,,3)",
                        "(,,1,2)", "(,,2,3)", "(1,1,,2)", "(1,2,,2)", "(2,2,,1)",
                        "(,1,1,1)", "(,1,2,1)", "(,2,1,1)", "(,2,2,2)", "(1,,1,2)",
                        "(1,,2,2)", "(2,,2,1)", "(,,,5)" };
        validate(rows, expected);
    }

    @Test
    void testMixedTypes() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        // members: srinivas, maneesh, krishna, saurabh, rui
        // dimensions: country code, number of monitors, vegetarian
        Object[][] rows =
                { { 1, 1, (long) 22222222222222L, 1 },
                        { 2, 1, (long) 22222222222222L, 2 },
                        { 3, 1, (long) 11111111111111L, 1 },
                        { 4, 1, (long) 11111111111111L, 2 },
                        { 5, 2, (long) 22222222222222L, 2 } };
        String[] expected =
                new String[] { "(1,22222222222222,1,1)", "(1,22222222222222,2,1)",
                        "(1,11111111111111,1,1)", "(1,11111111111111,2,1)",
                        "(2,22222222222222,2,1)", "(1,,,4)", "(2,,,1)",
                        "(,11111111111111,,2)", "(,22222222222222,,3)", "(,,1,2)",
                        "(,,2,3)", "(1,11111111111111,,2)", "(1,22222222222222,,2)",
                        "(2,22222222222222,,1)", "(,11111111111111,1,1)",
                        "(,11111111111111,2,1)", "(,22222222222222,1,1)",
                        "(,22222222222222,2,2)", "(1,,1,2)", "(1,,2,2)", "(2,,2,1)",
                        "(,,,5)" };
        validate(rows, expected);
    }

    @Test
    void testThreeDimsTeamGroupingSets() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        // members: srinivas, maneesh, krishna, saurabh, rui
        // dimensions: country code, number of monitors, vegetarian
        Object[][] rows =
                { { 1, (int) 1, (int) 2, (int) 1 }, { 2, (int) 1, (int) 2, (int) 2 },
                        { 3, (int) 1, (int) 1, (int) 1 },
                        { 4, (int) 1, (int) 1, (int) 2 },
                        { 5, (int) 2, (int) 2, (int) 2 } };
        String[] expected =
                new String[] { "(1,,,4)", "(2,,,1)", "(1,1,,2)", "(1,2,,2)", "(2,2,,1)" };
        validateGroupingSets(rows, expected, new String[] { "Dim0,Dim1", "Dim0" });
    }

    // There was not Assert statement in the original code.
    // Commenting it out for now
    // @SuppressWarnings("unused")
    // @Test
    void testThreeDimsTeamGroupingSetsMultiAggregate() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        // // members: srinivas, maneesh, krishna, saurabh, rui
        // // dimensions: country code, number of monitors, vegetarian
        // Object[][] rows =
        // { { 1, (int) 1, (int) 2, (int) 1, 1 },
        // { 2, (int) 1, (int) 2, (int) 2, 2 },
        // { 3, (int) 1, (int) 1, (int) 1, null },
        // { 4, (int) 1, (int) 1, (int) 2, null },
        // { 5, (int) 2, (int) 2, (int) 2, null } };
        //
        // String valueColumns = "member,condition";
        // int ndims = 3;
        // String[] groupingSets = new String[] { "Dim0", "Dim0,Dim1" };
        //
        // String[] dimensions = new String[ndims];
        // String[] columnNames = new String[ndims + valueColumns.split(",").length];
        // columnNames[0] = "member";
        // StringBuffer typeName = new StringBuffer();
        // for (int i = 0; i < ndims; i++)
        // {
        // if (i > 0)
        // typeName.append(",");
        // typeName.append("int ");
        // String name = "Dim" + i;
        // typeName.append(name);
        // columnNames[i + 1] = name;
        // dimensions[i] = name;
        // }
        //
        // BlockSchema blockSchema = new BlockSchema(typeName.toString());
        //
        // String[] fields = valueColumns.split(",");
        // for (int i = 1; i < fields.length; i++)
        // columnNames[ndims + i] = fields[i];
        //
        // Block block = new ArrayBlock(Arrays.asList(rows), columnNames, 1);
        //
        // HashMap<String, Block> map = new HashMap<String, Block>();
        // map.put("block", block);
        //
        // OLAPCubeCountDistinct cd = new OLAPCubeCountDistinct();
        //
        // ObjectMapper mapper = new ObjectMapper();
        // ObjectNode node = mapper.createObjectNode();
        //
        // ArrayNode measures = mapper.createArrayNode();
        // ObjectNode m1 = mapper.createObjectNode();
        // m1.put("input", "member");
        // m1.put("output", "LONG CDMember0");
        // measures.add(m1);
        // ObjectNode m2 = mapper.createObjectNode();
        // m2.put("input", "condition");
        // m2.put("output", "LONG CDMember1");
        // measures.add(m2);
        // node.put("aggregates", measures);
        //
        // ArrayNode dimensionNode = mapper.createArrayNode();
        // for (int i = 0; i < dimensions.length; i++)
        // {
        // dimensionNode.add(dimensions[i]);
        // }
        // node.put("dimensions", dimensionNode);
        //
        // ArrayNode groupingSetNode = mapper.createArrayNode();
        // if (groupingSets != null)
        // for (String str : groupingSets)
        // groupingSetNode.add(str);
        //
        // node.put("groupingSets", groupingSetNode);
        //
        // cd.setInput(map, node);
        //
        // // aggregate the output
        // Map<DimensionKey, int[]> aggregated = new HashMap<DimensionKey, int[]>();
        //
        // Tuple tuple;
        //
        // int sizeInInts =
        // OLAPCubeUtils.getDimensionsSizeInInteger(blockSchema, dimensions);
        //
        // while ((tuple = cd.next()) != null)
        // {
        // DimensionKey key = new DimensionKey(sizeInInts);
        // for (int i = 0; i < ndims; i++)
        // {
        // if (tuple.get(i) != null)
        // {
        // int code = ((Integer) tuple.get(i)).intValue();
        // key.set(i, code);
        // }
        // else
        // key.set(i, 0);
        // }
        //
        // int count_distinct = ((Long) tuple.get(ndims)).intValue();
        // int conditional_cd = ((Long) tuple.get(ndims + 1)).intValue();
        //
        // int[] old = aggregated.get(key);
        // if (old == null)
        // {
        // old = new int[2];
        // aggregated.put(key, old);
        // }
        // old[0] += count_distinct;
        // old[1] += conditional_cd;
        // }
        // for (DimensionKey k : aggregated.keySet())
        // {
        // print.f("key %s ==> value %s",
        // k.toString(),
        // Arrays.toString(aggregated.get(k)));
        // }
    }
}
