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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.TupleOperatorBlock;
import com.linkedin.cubert.operator.CombineOperator;
import com.linkedin.cubert.operator.DictionaryEncodeOperator;
import com.linkedin.cubert.operator.GroupByOperator;
import com.linkedin.cubert.operator.HashJoinOperator;
import com.linkedin.cubert.operator.MergeJoinOperator;
import com.linkedin.cubert.operator.PhaseContext;
import com.linkedin.cubert.operator.SortOperator;
import com.linkedin.cubert.operator.TupleOperator;
import com.linkedin.cubert.plan.physical.CubertStrings;

public class TestOperators
{

    @BeforeClass
    public void setUp() throws JsonGenerationException,
            JsonMappingException,
            IOException
    {
    }

    @SuppressWarnings("rawtypes")
    @BeforeTest
    void setupConf() throws IOException
    {
        Configuration conf = new JobConf();
        conf.setBoolean(CubertStrings.USE_COMPACT_SERIALIZATION, false);

        PhaseContext.create((Mapper.Context) null, conf);
        PhaseContext.create((Reducer.Context) null, conf);
    }

    public void testDictionaryEncoding() throws IOException,
            InterruptedException
    {
        // create dictionary block
        Object[][] dictRows = { { 1000, 100, 1 }, { 1000, 101, 2 } };
        Block dictionary =
                new ArrayBlock(Arrays.asList(dictRows), new String[] { "colname",
                        "colvalue", "code" });

        // create data block
        Object[][] dataRows = { { 100, 10 }, { 100, 11 }, { 101, 10 } };
        Block dataBlock =
                new ArrayBlock(Arrays.asList(dataRows), new String[] { "1000", "a" });

        // create operator
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", dictionary);
        input.put("block2", dataBlock);
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("dictionary", "block1");

        TupleOperator operator = new DictionaryEncodeOperator();
        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT 1000, INT a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        Object[][] expected = { { 1, 10 }, { 1, 11 }, { 2, 10 } };

        ArrayBlock.assertData(output, expected, new String[] { "1000", "a" });
    }

    @Test
    // when there are multiple rows in one table
    public void testLeftHashJoin() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 = { { 0 }, { 2 }, { 2 }, { 5 }, { 10 }, { 100 } };
        Object[][] rows2 = { { 1 }, { 2 }, { 7 }, { 9 }, { 100 }, { 100 } };
        Object[][] expected =
                { { 0, null }, { 2, 2 }, { 2, 2 }, { 5, null }, { 10, null },
                        { 100, 100 }, { 100, 100 } };

        Block block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a" });
        Block block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "a" });

        TupleOperator operator = new HashJoinOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block1);
        input.put("block2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("leftJoinKeys", "a");
        node.put("rightJoinKeys", "a");
        node.put("leftBlock", "block1");
        node.put("rightBlock", "block2");
        node.put("joinType", "left outer");

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT block1___a, INT block2___a"),
                                    (BlockProperties) null);

        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, expected, new String[] { "block1.a", "block2.a" });
    }

    @Test
    // when there are multiple rows in one table
    public void testLeftMergeJoin() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 = { { 0 }, { 2 }, { 2 }, { 5 }, { 10 }, { 100 } };
        Object[][] rows2 = { { 1 }, { 2 }, { 7 }, { 9 }, { 100 }, { 100 } };
        Object[][] expected =
                { { 0, null }, { 2, 2 }, { 2, 2 }, { 5, null }, { 10, null },
                        { 100, 100 }, { 100, 100 } };

        Block block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a" });
        Block block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "a" });

        TupleOperator operator = new MergeJoinOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block1);
        input.put("block2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("leftCubeColumns", "a");
        node.put("rightCubeColumns", "a");
        node.put("leftBlock", "block1");
        node.put("rightBlock", "block2");
        node.put("joinType", "left outer");

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT block1___a, INT block2___a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, expected, new String[] { "block1.a", "block2.a" });
    }

    @Test
    // when there are multiple rows in one table
    public void testRightMergeJoin() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 = { { 0 }, { 2 }, { 2 }, { 5 }, { 10 }, { 100 } };
        Object[][] rows2 = { { 1 }, { 2 }, { 7 }, { 9 }, { 100 }, { 100 } };
        Object[][] expected =
                { { null, 1 }, { 2, 2 }, { 2, 2 }, { null, 7 }, { null, 9 },
                        { 100, 100 }, { 100, 100 } };

        Block block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a" });
        Block block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "a" });

        TupleOperator operator = new MergeJoinOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block1);
        input.put("block2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("leftCubeColumns", "a");
        node.put("rightCubeColumns", "a");
        node.put("leftBlock", "block1");
        node.put("rightBlock", "block2");
        node.put("joinType", "right outer");
        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT block1___a, INT block2___a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, expected, new String[] { "block1.a", "block2.a" });
    }

//    @Test
    // when there are multiple rows in one table
    public void testRightHashJoin() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 = { { 0 }, { 2 }, { 2 }, { 5 }, { 10 }, { 100 } };
        Object[][] rows2 = { { 1 }, { 2 }, { 7 }, { 9 }, { 100 }, { 100 } };
        Object[][] expected =
                { { 2, 2 }, { 2, 2 }, { 100, 100 }, { 100, 100 }, { null, 7 },
                        { null, 1 }, { null, 9 }, };

        Block block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a" });
        Block block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "a" });

        TupleOperator operator = new HashJoinOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block1);
        input.put("block2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("leftJoinKeys", "a");
        node.put("rightJoinKeys", "a");
        node.put("leftBlock", "block1");
        node.put("rightBlock", "block2");
        node.put("joinType", "right outer");
        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT block1___a, INT block2___a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, expected, new String[] { "block1.a", "block2.a" });
    }

    @Test
    // when there are multiple rows in one table
    public void testMergeJoinFullOuter() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 = { { 0 }, { 2 }, { 2 }, { 5 }, { 10 }, { 100 } };
        Object[][] rows2 = { { 1 }, { 2 }, { 7 }, { 9 }, { 100 }, { 100 } };
        Object[][] expected =
                { { 0, null }, { null, 1 }, { 2, 2 }, { 2, 2 }, { 5, null }, { null, 7 },
                        { null, 9 }, { 10, null }, { 100, 100 }, { 100, 100 } };

        Block block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a" });
        Block block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "a" });

        TupleOperator operator = new MergeJoinOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block1);
        input.put("block2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("leftCubeColumns", "a");
        node.put("rightCubeColumns", "a");
        node.put("leftBlock", "block1");
        node.put("rightBlock", "block2");
        node.put("joinType", "full outer");
        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT block1___a, INT block2___a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, expected, new String[] { "block1.a", "block2.a" });
        System.out.println("Successfully tested MERGE JOIN FULL OUTER");
    }

    @Test
    public void testMergeJoinFullOuterEmptyRight() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 = { { 0 }, { 2 }, { 2 }, { 5 }, { 10 }, { 100 } };
        Object[][] rows2 = {};
        Object[][] expected =
                { { 0, null }, { 2, null }, { 2, null }, { 5, null }, { 10, null },
                        { 100, null } };

        Block block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a" });
        Block block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "a" });

        TupleOperator operator = new MergeJoinOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block1);
        input.put("block2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("leftCubeColumns", "a");
        node.put("rightCubeColumns", "a");
        node.put("leftBlock", "block1");
        node.put("rightBlock", "block2");
        node.put("joinType", "full outer");
        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT block1___a, INT block2___a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, expected, new String[] { "block1.a", "block2.a" });
        System.out.println("Successfully tested MERGE JOIN FULL OUTER empty block");
    }

    @Test
    public void testGroupByWith2Dimensions() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 =
                { { 0, 0, 100 }, { 0, 0, 100 }, { 2, 2, 100 }, { 2, 5, 100 },
                        { 5, 6, 100 }, { 6, 6, 100 } };
        Block block =
                new ArrayBlock(Arrays.asList(rows1), new String[] { "a", "b", "c" }, 1);
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block", block);

        TupleOperator operator = new GroupByOperator();
        ObjectMapper mapper = new ObjectMapper();

        ObjectNode json = mapper.createObjectNode();
        json.put("input", "block");
        ArrayNode anode = mapper.createArrayNode();
        anode.add("a");
        anode.add("b");
        json.put("groupBy", anode);
        anode = mapper.createArrayNode();
        ObjectNode onode = mapper.createObjectNode();
        onode.put("type", "SUM");
        onode.put("input", "c");
        onode.put("output", "sum");
        anode.add(onode);
        json.put("aggregates", anode);

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT a, INT b, INT sum"),
                                    (BlockProperties) null);
        operator.setInput(input, json, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, new Object[][] { { 0, 0, 200 }, { 2, 2, 100 },
                { 2, 5, 100 }, { 5, 6, 100 }, { 6, 6, 100 } }, new String[] { "a", "b",
                "sum" });
    }

    @Test
    public void testGroupByDedup() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 =
                { { 0, 0 }, { 0, 1 }, { 0, 1 }, { 5, 6 }, { 5, 6 }, { 100, 10 } };
        Block block = new ArrayBlock(Arrays.asList(rows1), new String[] { "a", "b" }, 1);
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block", block);

        TupleOperator operator = new GroupByOperator();
        ObjectMapper mapper = new ObjectMapper();

        ObjectNode json = mapper.createObjectNode();
        json.put("input", "block");
        ArrayNode anode = mapper.createArrayNode();
        anode.add("a");
        anode.add("b");
        json.put("groupBy", anode);

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT a, INT b"),
                                    (BlockProperties) null);
        operator.setInput(input, json, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, new Object[][] { { 0, 0 }, { 0, 1 }, { 5, 6 },
                { 100, 10 } }, new String[] { "a", "b" });
    }

    @Test
    public void testCombinedBlockRightRunsout() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 = { { 3 }, { 5 }, { 10 } };
        Block block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a" }, 1);

        Object[][] rows2 = { { 2 }, { 7 }, { 9 } };
        Block block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "a" }, 1);

        TupleOperator operator = new CombineOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("cube1", block1);
        input.put("cube2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        JsonNode ct = mapper.readValue("[\"a\"]", JsonNode.class);
        node.put("pivotBy", ct);

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, new Object[][] { { 2 }, { 3 }, { 5 }, { 7 }, { 9 },
                { 10 } }, new String[] { "a" });
    }

    @Test
    public void testCombinedBlockLeftRunsout() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 = { { 2 }, { 7 }, { 9 } };
        Object[][] rows2 = { { 3 }, { 5 }, { 10 } };

        Block block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a" }, 1);
        Block block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "a" }, 1);

        TupleOperator operator = new CombineOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("cube1", block1);
        input.put("cube2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        JsonNode ct = mapper.readValue("[\"a\"]", JsonNode.class);
        node.put("pivotBy", ct);

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, new Object[][] { { 2 }, { 3 }, { 5 }, { 7 }, { 9 },
                { 10 } }, new String[] { "a" });
    }

    @Test
    // when there are multiple rows in one table
    public void testHashJoin1() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 = { { 2 }, { 2 }, { 7 }, { 9 } };
        Object[][] rows2 = { { 2 }, { 7 }, { 9 } };
        Object[][] expected = { { 2, 2 }, { 2, 2 }, { 7, 7 }, { 9, 9 } };

        ArrayBlock block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a" });
        ArrayBlock block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "a" });

        TupleOperator operator = new HashJoinOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block1);
        input.put("block2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("leftJoinKeys", "a");
        node.put("rightJoinKeys", "a");
        node.put("leftBlock", "block1");

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT block1___a, INT block2___a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, expected, new String[] { "block1.a", "block2.a" });
    }

    @Test
    // when there are multiple rows in one table
    public void testHashJoin2() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 = { { 2 }, { 2 }, { 5 }, { 10 } };
        Object[][] rows2 = { { 1 }, { 7 }, { 9 } };
        Object[][] expected = {};

        Block block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a" });
        Block block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "a" });

        TupleOperator operator = new HashJoinOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block1);
        input.put("block2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("leftJoinKeys", "a");
        node.put("rightJoinKeys", "a");
        node.put("leftBlock", "block1");

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT block1___a, INT block2___a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, expected, new String[] { "block1.a", "block2.a" });
    }

    @Test
    // when there are multiple rows in one table
    public void testHashJoin3() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 = { { 0 }, { 2 }, { 2 }, { 5 }, { 10 } };
        Object[][] rows2 = { { 1 }, { 2 }, { 2 }, { 7 }, { 9 } };
        Object[][] expected = { { 2, 2 }, { 2, 2 }, { 2, 2 }, { 2, 2 } };

        Block block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a" });
        Block block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "a" });

        TupleOperator operator = new HashJoinOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block1);
        input.put("block2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("leftJoinKeys", "a");
        node.put("rightJoinKeys", "a");
        node.put("leftBlock", "block1");

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT block1___a, INT block2___a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, expected, new String[] { "block1.a", "block2.a" });
    }

    @Test
    // when there are multiple rows in one table
    public void testHashJoin4() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 = { { 0 }, { 2 }, { 2 }, { 5 }, { 10 }, { 100 } };
        Object[][] rows2 = { { 1 }, { 2 }, { 7 }, { 9 }, { 100 }, { 100 } };
        Object[][] expected = { { 2, 2 }, { 2, 2 }, { 100, 100 }, { 100, 100 } };

        Block block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a" });
        Block block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "a" });

        TupleOperator operator = new HashJoinOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block1);
        input.put("block2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("leftJoinKeys", "a");
        node.put("rightJoinKeys", "a");
        node.put("leftBlock", "block1");

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT block1___a, INT block2___a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, expected, new String[] { "block1.a", "block2.a" });
    }

    @Test
    // testing multiple join keys
    public void testHashJoinMultipleJoinKeys() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 =
                { { 0, 1 }, { 2, 1 }, { 2, 2 }, { 5, 1 }, { 10, 1 }, { 100, 1 } };
        Object[][] rows2 =
                { { 1, 1 }, { 2, 0 }, { 2, 1 }, { 5, 1 }, { 100, 2 }, { 100, 3 } };
        Object[][] expected = { { 2, 1, 2, 1 }, { 5, 1, 5, 1 } };

        Block block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a", "b" });
        Block block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "c", "a" });

        TupleOperator operator = new HashJoinOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block1);
        input.put("block2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        ArrayNode lkeys = mapper.createArrayNode();
        lkeys.add("a");
        lkeys.add("b");
        node.put("leftJoinKeys", lkeys);
        ArrayNode rkeys = mapper.createArrayNode();
        rkeys.add("c");
        rkeys.add("a");
        node.put("rightJoinKeys", rkeys);
        node.put("leftBlock", "block1");

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT block1___a, INT block1___b, INT block2___c, INT block2___a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, expected, new String[] { "block1.a", "block2.a" });
    }

    @Test
    // testing multiple join keys
    public void testHashJoinMultipleJoinKeysInDifferentOrder() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 =
                { { 0, 1 }, { 2, 1 }, { 2, 2 }, { 5, 1 }, { 10, 1 }, { 100, 1 } };
        Object[][] rows2 =
                { { 1, 1 }, { 2, 0 }, { 2, 1 }, { 5, 1 }, { 100, 2 }, { 100, 3 } };
        Object[][] expected = { { 0, 1, 2, 0 }, { 2, 1, 100, 2 }, { 2, 2, 100, 2 } };

        Block block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a", "b" });
        Block block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "a", "c" });

        TupleOperator operator = new HashJoinOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block1);
        input.put("block2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        ArrayNode lkeys = mapper.createArrayNode();
        lkeys.add("a");
        node.put("leftJoinKeys", lkeys);
        ArrayNode rkeys = mapper.createArrayNode();
        rkeys.add("c");
        node.put("rightJoinKeys", rkeys);
        node.put("leftBlock", "block1");

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT block1___a, INT block1___b, INT block2___c, INT block2___a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, expected, new String[] { "block1.a", "block2.c" });
    }

    @Test
    // testing multiple join keys
    public void testMergeJoinMultipleJoinKeys() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 =
                { { 0, 1 }, { 2, 1 }, { 2, 2 }, { 5, 1 }, { 10, 1 }, { 100, 1 } };
        Object[][] rows2 =
                { { 1, 1 }, { 2, 0 }, { 2, 1 }, { 5, 1 }, { 100, 2 }, { 100, 3 } };
        Object[][] expected = { { 2, 1, 2, 1 }, { 5, 1, 5, 1 } };

        Block block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a", "b" });
        Block block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "c", "a" });

        TupleOperator operator = new MergeJoinOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block1);
        input.put("block2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        ArrayNode lkeys = mapper.createArrayNode();
        lkeys.add("a");
        lkeys.add("b");
        node.put("leftCubeColumns", lkeys);
        ArrayNode rkeys = mapper.createArrayNode();
        rkeys.add("c");
        rkeys.add("a");
        node.put("rightCubeColumns", rkeys);
        node.put("leftBlock", "block1");

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT block1___a, INT block1___b, INT block2___c, INT block2___a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, expected, new String[] { "block1.a", "block2.a" });
    }

    @Test
    // when there are multiple rows in one table
    public void testMergeJoin1() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 = { { 2 }, { 2 }, { 7 }, { 9 } };
        Object[][] rows2 = { { 2 }, { 7 }, { 9 } };
        Object[][] expected = { { 2, 2 }, { 2, 2 }, { 7, 7 }, { 9, 9 } };

        ArrayBlock block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a" });
        ArrayBlock block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "a" });

        TupleOperator operator = new MergeJoinOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block1);
        input.put("block2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("leftCubeColumns", "a");
        node.put("rightCubeColumns", "a");
        node.put("leftBlock", "block1");

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT block1___a, INT block2___a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, expected, new String[] { "block1.a", "block2.a" });
    }

    @Test
    // when there are multiple rows in one table
    public void testMergeJoin2() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 = { { 2 }, { 2 }, { 5 }, { 10 } };
        Object[][] rows2 = { { 1 }, { 7 }, { 9 } };
        Object[][] expected = {};

        Block block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a" });
        Block block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "a" });

        TupleOperator operator = new MergeJoinOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block1);
        input.put("block2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("leftCubeColumns", "a");
        node.put("rightCubeColumns", "a");
        node.put("leftBlock", "block1");

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT block1___a, INT block2___a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, expected, new String[] { "block1.a", "block2.a" });
    }

    @Test
    // when there are multiple rows in one table
    public void testMergeJoin3() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 = { { 0 }, { 2 }, { 2 }, { 5 }, { 10 } };
        Object[][] rows2 = { { 1 }, { 2 }, { 2 }, { 7 }, { 9 } };
        Object[][] expected = { { 2, 2 }, { 2, 2 }, { 2, 2 }, { 2, 2 } };

        Block block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a" });
        Block block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "a" });

        TupleOperator operator = new MergeJoinOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block1);
        input.put("block2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("leftCubeColumns", "a");
        node.put("rightCubeColumns", "a");
        node.put("leftBlock", "block1");

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT block1___a, INT block2___a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, expected, new String[] { "block1.a", "block2.a" });
    }

    @Test
    // when there are multiple rows in one table
    public void testMergeJoin4() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 = { { 0 }, { 2 }, { 2 }, { 5 }, { 10 }, { 100 } };
        Object[][] rows2 = { { 1 }, { 2 }, { 7 }, { 9 }, { 100 }, { 100 } };
        Object[][] expected = { { 2, 2 }, { 2, 2 }, { 100, 100 }, { 100, 100 } };

        Block block1 = new ArrayBlock(Arrays.asList(rows1), new String[] { "a" });
        Block block2 = new ArrayBlock(Arrays.asList(rows2), new String[] { "a" });

        TupleOperator operator = new MergeJoinOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("block1", block1);
        input.put("block2", block2);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("leftCubeColumns", "a");
        node.put("rightCubeColumns", "a");
        node.put("leftBlock", "block1");

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT block1___a, INT block2___a"),
                                    (BlockProperties) null);
        operator.setInput(input, node, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, expected, new String[] { "block1.a", "block2.a" });
    }

    @Test
    public void testGroupByWithSum1() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 = { { 0 }, { 2 }, { 2 }, { 5 }, { 10 }, { 100 } };
        Block block = new ArrayBlock(Arrays.asList(rows1), new String[] { "a" }, 1);

        TupleOperator operator = new GroupByOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("first", block);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode json = mapper.createObjectNode();
        json.put("input", "first");
        ArrayNode anode = mapper.createArrayNode();
        anode.add("a");
        json.put("groupBy", anode);
        anode = mapper.createArrayNode();
        ObjectNode onode = mapper.createObjectNode();
        onode.put("type", "SUM");
        onode.put("input", "a");
        onode.put("output", "sum");
        anode.add(onode);
        json.put("aggregates", anode);

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT a, INT sum"),
                                    (BlockProperties) null);
        operator.setInput(input, json, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, new Object[][] { { 0, 0 }, { 2, 4 }, { 5, 5 },
                { 10, 10 }, { 100, 100 } }, new String[] { "a", "sum" });
    }

    @Test
    public void testGroupByWithMin() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 =
                { { 0, 0 }, { 2, 2 }, { 2, 5 }, { 5, 6 }, { 10, 1 }, { 10, 20 },
                        { 100, 10 }, { 100, 1 } };
        Block block = new ArrayBlock(Arrays.asList(rows1), new String[] { "a", "b" }, 1);

        TupleOperator operator = new GroupByOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("first", block);

        ObjectMapper mapper = new ObjectMapper();

        ObjectNode json = mapper.createObjectNode();
        json.put("input", "first");
        ArrayNode anode = mapper.createArrayNode();
        anode.add("a");
        json.put("groupBy", anode);
        anode = mapper.createArrayNode();
        ObjectNode onode = mapper.createObjectNode();
        onode.put("type", "MIN");
        onode.put("input", "b");
        onode.put("output", "min");
        anode.add(onode);
        json.put("aggregates", anode);

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT a, INT min"),
                                    (BlockProperties) null);
        operator.setInput(input, json, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, new Object[][] { { 0, 0 }, { 2, 2 }, { 5, 6 },
                { 10, 1 }, { 100, 1 } }, new String[] { "a", "min" });
    }

    @Test
    public void testGroupByWithCount() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 =
                { { 0, 0 }, { 2, 2 }, { 2, 5 }, { 5, 6 }, { 10, 1 }, { 10, 20 },
                        { 100, 10 }, { 100, 1 } };
        Block block = new ArrayBlock(Arrays.asList(rows1), new String[] { "a", "b" }, 1);

        TupleOperator operator = new GroupByOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("first", block);

        ObjectMapper mapper = new ObjectMapper();

        ObjectNode json = mapper.createObjectNode();
        json.put("input", "first");
        ArrayNode anode = mapper.createArrayNode();
        anode.add("a");
        json.put("groupBy", anode);
        anode = mapper.createArrayNode();
        ObjectNode onode = mapper.createObjectNode();
        onode.put("type", "COUNT");
        onode.put("input", "b");
        onode.put("output", "countCol");
        anode.add(onode);
        json.put("aggregates", anode);

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT a, LONG countCol"),
                                    (BlockProperties) null);
        operator.setInput(input, json, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, new Object[][] { { 0, 1L }, { 2, 2L }, { 5, 1L },
                { 10, 2L }, { 100, 2L } }, new String[] { "a", "countCol" });
    }

    @Test
    public void testGroupByWithMax() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 =
                { { 0, 0 }, { 2, 2 }, { 2, 5 }, { 5, 6 }, { 10, 1 }, { 10, 20 },
                        { 100, 10 }, { 100, 1 } };
        Block block = new ArrayBlock(Arrays.asList(rows1), new String[] { "a", "b" }, 1);

        TupleOperator operator = new GroupByOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("first", block);

        ObjectMapper mapper = new ObjectMapper();

        ObjectNode json = mapper.createObjectNode();
        json.put("input", "first");
        ArrayNode anode = mapper.createArrayNode();
        anode.add("a");
        json.put("groupBy", anode);
        anode = mapper.createArrayNode();
        ObjectNode onode = mapper.createObjectNode();
        onode.put("type", "MAX");
        onode.put("input", "b");
        onode.put("output", "max");
        anode.add(onode);
        json.put("aggregates", anode);

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT a, INT max"),
                                    (BlockProperties) null);
        operator.setInput(input, json, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, new Object[][] { { 0, 0 }, { 2, 5 }, { 5, 6 },
                { 10, 20 }, { 100, 10 } }, new String[] { "a", "max" });
    }

    @Test
    public void testGroupByWithSum2() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        Object[][] rows1 =
                { { 0, 0 }, { 2, 2 }, { 2, 5 }, { 5, 6 }, { 10, 1 }, { 100, 10 } };
        Block block = new ArrayBlock(Arrays.asList(rows1), new String[] { "a", "b" }, 1);

        TupleOperator operator = new GroupByOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("first", block);

        ObjectMapper mapper = new ObjectMapper();

        ObjectNode json = mapper.createObjectNode();
        json.put("input", "first");
        ArrayNode anode = mapper.createArrayNode();
        anode.add("a");
        json.put("groupBy", anode);
        anode = mapper.createArrayNode();
        ObjectNode onode = mapper.createObjectNode();
        onode.put("type", "SUM");
        onode.put("input", "b");
        onode.put("output", "sum");
        anode.add(onode);
        json.put("aggregates", anode);

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT a, INT sum"),
                                    (BlockProperties) null);
        operator.setInput(input, json, props);

        Block output = new TupleOperatorBlock(operator, props);

        ArrayBlock.assertData(output, new Object[][] { { 0, 0 }, { 2, 7 }, { 5, 6 },
                { 10, 1 }, { 100, 10 } }, new String[] { "a", "sum" });
    }

    @Test
    public void testSortOperator() throws JsonGenerationException,
            JsonMappingException,
            IOException,
            InterruptedException
    {
        System.out.println("Testing SORT operator");

        Object[][] rows1 =
                { { 0, 10, 0 }, { 2, 5, 2 }, { 2, 8, 5 }, { 5, 9, 6 }, { 10, 11, 1 },
                        { 100, 6, 10 } };
        Block block =
                new ArrayBlock(Arrays.asList(rows1), new String[] { "a", "b", "c" }, 1);

        TupleOperator operator = new SortOperator();
        Map<String, Block> input = new HashMap<String, Block>();
        input.put("unsorted", block);

        ObjectMapper mapper = new ObjectMapper();

        ObjectNode json = mapper.createObjectNode();
        json.put("input", "unsorted");
        ArrayNode anode = mapper.createArrayNode();
        anode.add("b");
        anode.add("c");
        json.put("sortBy", anode);

        BlockProperties props =
                new BlockProperties(null,
                                    new BlockSchema("INT a, INT b, INT c"),
                                    (BlockProperties) null);
        operator.setInput(input, json, props);

        Block output = new TupleOperatorBlock(operator, props);

        System.out.println("output is " + output);
        ArrayBlock.assertData(output, new Object[][] { { 2, 5, 2 }, { 100, 6, 10 },
                { 2, 8, 5 }, { 5, 9, 6 }, { 0, 10, 0 }, { 10, 11, 1 } }, new String[] {
                "a", "b", "c" });
    }

}
