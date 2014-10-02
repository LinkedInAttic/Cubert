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

/**
 * Sample the tuples from a data source.
 * 
 * Used for debugging to cut down the size of data.
 * 
 * Currently the implemention is simple: pick the first N tuple. Later on we can expand it
 * to probabilistic sampling etc.
 * 
 * @author Maneesh Varshney
 * 
 */
public class LimitOperator implements TupleOperator
{
    private Block block;
    private int maxTuples;
    private int numTuples;

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        block = input.values().iterator().next();
        maxTuples = json.get("maxTuples").getIntValue();
        numTuples = 0;
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        if (numTuples >= maxTuples)
        {
            // we are done.. but first drain the remaining tuples
            while (block.next() != null)
            {

            }
            return null;
        }

        numTuples++;
        return block.next();
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        return preConditions.values().iterator().next();
    }
}
