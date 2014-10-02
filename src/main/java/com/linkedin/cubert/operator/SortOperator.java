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
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.SerializedTupleStore;

/***
 * 
 * An operator that sorts the given block on a given set of keys and returns the sorted
 * data.
 * 
 * @author Krishna Puttaswamy
 * 
 */
public class SortOperator implements TupleOperator
{
    private Block block;
    private Iterator<Tuple> iterator;

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        block = input.values().iterator().next();

        // get the input block
        Block dataBlock = input.values().iterator().next();

        // create pivoted block
        String[] sortByColumns = JsonUtils.asArray(json, "sortBy");

        BlockSchema outputSchema = props.getSchema();
        SerializedTupleStore store =
                new SerializedTupleStore(outputSchema, sortByColumns);

        Tuple tuple;
        while ((tuple = block.next()) != null)
            store.addToStore(tuple);
        store.sort();

        iterator = store.iterator();
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        if (iterator.hasNext())
            return iterator.next();
        else
            return null;
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        PostCondition condition = preConditions.values().iterator().next();
        BlockSchema inputSchema = condition.getSchema();
        String[] partitionKeys = condition.getPartitionKeys();
        String[] sortKeys = JsonUtils.asArray(json, "sortBy");

        return new PostCondition(inputSchema, partitionKeys, sortKeys);
    }
}
