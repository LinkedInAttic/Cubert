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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.functions.FunctionTree;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * Implements the GENERATE operator.
 * 
 * @author Maneesh Varshney
 * 
 */
public class GenerateOperator implements TupleOperator
{
    private Block block;
    private Tuple outputTuple;
    private FunctionTree functionTree;

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        /* Retrieve the input block */
        if (input.isEmpty())
            throw new RuntimeException("Block is not provided.");

        block = (Block) input.values().iterator().next();

        if (block == null)
            throw new RuntimeException("Input block is null at " + json.get("line"));

        functionTree = new FunctionTree(block);

        try
        {
            BlockSchema outputSchema =
                    buildFunctionTree(functionTree, json.get("outputTuple"));
            outputTuple =
                    TupleFactory.getInstance().newTuple(outputSchema.getNumColumns());
        }
        catch (PreconditionException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static BlockSchema buildFunctionTree(FunctionTree tree, JsonNode json) throws PreconditionException
    {
        int numOutputColumns = json.size();
        ColumnType[] outputTypes = new ColumnType[numOutputColumns];

        for (int i = 0; i < numOutputColumns; i++)
        {
            JsonNode outputColJson = json.get(i);
            String colName = JsonUtils.getText(outputColJson, "col_name");
            tree.addFunctionTree(outputColJson.get("expression"));

            ColumnType type = tree.getType(i);
            ColumnType copy = new ColumnType(colName, type.getType());
            copy.setColumnSchema(type.getColumnSchema());
            outputTypes[i] = copy;
        }

        return new BlockSchema(outputTypes);
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        Tuple tuple = block.next();
        if (tuple == null)
            return null;

        functionTree.attachTuple(tuple);
        for (int i = 0; i < outputTuple.size(); i++)
            outputTuple.set(i, functionTree.evalTree(i));

        return outputTuple;
    }

    /**
     * This method is used by the TEE operator. The code is copied (from the next()
     * method) so that we don't make an extra function call from the next() method.
     * 
     * @param tuple
     * @return
     * @throws IOException
     */
    public Tuple next(Tuple tuple) throws IOException
    {
        functionTree.attachTuple(tuple);
        for (int i = 0; i < outputTuple.size(); i++)
            outputTuple.set(i, functionTree.evalTree(i));

        return outputTuple;
    }

    private Map<String, String> getLineage(BlockSchema inputSchema, JsonNode json) throws PreconditionException
    {
        Map<String, String> lineage = new HashMap<String, String>();
        for (JsonNode outputCol : json.path("outputTuple"))
        {
            String outColName = JsonUtils.getText(outputCol, "col_name");
            if (!outputCol.has("expression"))
                continue;

            JsonNode expressionJson = outputCol.get("expression");
            if (!expressionJson.has("function"))
                continue;

            String function = JsonUtils.getText(expressionJson, "function");
            if (!function.equals("INPUT_PROJECTION"))
                continue;
            Object selector =
                    JsonUtils.decodeConstant(expressionJson.get("arguments").get(0), null);
            int index = FunctionTree.getSelectorIndex(inputSchema, selector);

            String inColName = inputSchema.getColumnNames()[index];

            lineage.put(inColName, outColName);
        }
        return lineage;
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        PostCondition preCondition = preConditions.values().iterator().next();
        BlockSchema inputSchema = preCondition.getSchema();

        FunctionTree functionTree = new FunctionTree(inputSchema);
        BlockSchema outputSchema =
                buildFunctionTree(functionTree, json.get("outputTuple"));

        // infer the partition keys and sort keys (if they are renamed in this generate
        // operator)
        String[] partitionKeys = null;
        String[] sortKeys = null;
        Map<String, String> lineage = getLineage(inputSchema, json);
        if (preCondition.getPartitionKeys() != null)
        {
            ArrayList<String> postPartitionKeys = new ArrayList<String>();
            for (String key : preCondition.getPartitionKeys())
            {
                String name = lineage.get(key);
                if (name == null)
                    break;
                postPartitionKeys.add(name);
            }
            partitionKeys = postPartitionKeys.toArray(new String[0]);
        }

        if (preCondition.getSortKeys() != null)
        {
            ArrayList<String> postSortKeys = new ArrayList<String>();

            for (String key : preCondition.getSortKeys())
            {
                String name = lineage.get(key);
                if (name == null)
                    break;
                postSortKeys.add(name);
            }
            sortKeys = postSortKeys.toArray(new String[0]);
        }

        return new PostCondition(outputSchema, partitionKeys, sortKeys);

    }
}
