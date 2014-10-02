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

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.functions.FunctionTree;
import com.linkedin.cubert.operator.PreconditionException;

public class TestFunctionTree
{
    private final ObjectMapper mapper = new ObjectMapper();
    private static final ColumnType INT_TYPE = new ColumnType(null, DataType.INT);
    private static final ColumnType LONG_TYPE = new ColumnType(null, DataType.LONG);
    private static final ColumnType FLOAT_TYPE = new ColumnType(null, DataType.FLOAT);
    private static final ColumnType DOUBLE_TYPE = new ColumnType(null, DataType.DOUBLE);

    JsonNode toJson(String str)
    {
        str = str.replaceAll("'", "\"");
        try
        {
            return mapper.readValue(str, JsonNode.class);
        }
        catch (JsonParseException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (JsonMappingException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    JsonNode toJson(String template, Object... args)
    {
        return toJson(String.format(template, args));
    }

    @BeforeClass
    public void setUp() throws JsonGenerationException,
            JsonMappingException,
            IOException
    {
    }

    void doTest(BlockSchema inputSchema,
                JsonNode json,
                Object[][] data,
                ColumnType expectedType,
                Object[] expectedResults) throws PreconditionException,
            IOException
    {
        FunctionTree tree = new FunctionTree(inputSchema);
        tree.addFunctionTree(json);
        ColumnType outType = tree.getType(0);
        Assert.assertEquals(outType.getType(), expectedType.getType());

        int ntuples = data.length;
        int nfields = data[0].length;

        Tuple tuple = TupleFactory.getInstance().newTuple(nfields);

        for (int i = 0; i < ntuples; i++)
        {
            for (int j = 0; j < nfields; j++)
                tuple.set(j, data[i][j]);

            tree.attachTuple(tuple);
            Object out = tree.evalTree(0);
            Assert.assertEquals(out, expectedResults[i]);
        }
    }

    void test(BlockSchema inputSchema,
              JsonNode json,
              Object[][] data,
              ColumnType expectedType,
              Object[] expectedResults)
    {
        try
        {
            doTest(inputSchema, json, data, expectedType, expectedResults);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.assertFalse(true);
        }
    }

    void testFail(BlockSchema inputSchema,
                  JsonNode json,
                  Object[][] data,
                  ColumnType expectedType,
                  Object[] expectedResults)
    {
        try
        {
            doTest(inputSchema, json, data, expectedType, expectedResults);
            Assert.assertFalse(true);
        }
        catch (PreconditionException e)
        {
            return;
        }
        catch (Exception e)
        {
            Assert.assertFalse(true);
        }

    }

    @Test
    void testInputProjection()
    {
        String template = "{'function': 'INPUT_PROJECTION', 'arguments': [%s]}";

        JsonNode json;
        BlockSchema schema = new BlockSchema("INT col1");
        Object[][] data = new Object[][] { { 0 }, { null } };
        Object[] expected = new Object[] { 0, null };

        json = toJson(template, "'col1'");
        test(schema, json, data, INT_TYPE, expected);

        json = toJson(template, "0");
        test(schema, json, data, INT_TYPE, expected);

        json = toJson(template, "'col2'");
        testFail(schema, json, data, INT_TYPE, expected);

        json = toJson(template, "1");
        testFail(schema, json, data, INT_TYPE, expected);

    }

    @Test
    void testArithmetic()
    {
        String template =
                "{'function': '%s', 'arguments': ["
                        + "{'function': 'INPUT_PROJECTION', 'arguments': ['%s']},"
                        + "{'function': 'INPUT_PROJECTION', 'arguments': ['%s']}]}";
        JsonNode json;

        BlockSchema schema = new BlockSchema("INT col1, INT col2");
        Object[][] data =
                new Object[][] { { 10, 20 }, { 10, null }, { null, 20 }, { null, null } };

        // when both types are same
        json = toJson(template, "ADD", "col1", "col2");
        test(schema, json, data, INT_TYPE, new Object[] { 30, null, null, null });

        // when the two types are not same
        schema = new BlockSchema("int col1, long col2");
        data =
                new Object[][] { { 10, 20L }, { 10, null }, { null, 20L }, { null, null } };
        json = toJson(template, "ADD", "col1", "col2");
        test(schema, json, data, LONG_TYPE, new Object[] { 30L, null, null, null });

        // when the two types are incompatible
        schema = new BlockSchema("int col1, string col2");
        testFail(schema, json, data, null, null);

        // when the actual data is of less wider type
        schema = new BlockSchema("double col1, double col2");
        data = new Object[][] { { 10, 20L }, { 10L, 20f }, { 10f, 20d }, { 10d, 20 } };
        json = toJson(template, "ADD", "col1", "col2");
        test(schema, json, data, DOUBLE_TYPE, new Object[] { 30d, 30d, 30d, 30d });
    }
}
