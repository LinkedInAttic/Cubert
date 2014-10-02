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
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.functions.FunctionTree;

public class FilterOperator implements TupleOperator
{
    private Block block;
    private FunctionTree functionTree;

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        block = input.values().iterator().next();

        JsonNode filterJson = json.get("filter");

        functionTree = new FunctionTree(block);
        try
        {
            functionTree.addFunctionTree(filterJson);
        }
        catch (PreconditionException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        Tuple t;
        while ((t = block.next()) != null)
        {
            functionTree.attachTuple(t);
            Boolean val = (Boolean) functionTree.evalTree(0);

            if (val != null && val)
                return t;
        }
        return null;
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        PostCondition preCondition = preConditions.values().iterator().next();

        FunctionTree tree = new FunctionTree(preCondition.getSchema());
        tree.addFunctionTree(json.get("filter"));

        return preCondition;
    }
}
