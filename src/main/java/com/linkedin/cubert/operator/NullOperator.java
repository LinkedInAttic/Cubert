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
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * An operator that does nothing.
 * 
 * @author Maneesh Varshney
 * 
 */
public class NullOperator implements TupleOperator
{
    private Block block;

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        if (input.size() == 1)
        {
            block = input.values().iterator().next();
        }
        else
        {
            String inputBlockName = JsonUtils.getText(json, "mainInput");
            block = input.get(inputBlockName);
        }
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        return block.next();
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        PostCondition condition = preConditions.values().iterator().next();

        if (json.has("assertPartitionKeys"))
        {
            String[] assertPartitionKeys = JsonUtils.asArray(json, "assertPartitionKeys");
            if (!Arrays.equals(assertPartitionKeys, condition.getPartitionKeys()))
            {
                String msg =
                        String.format("Expected=%s. Found=%s",
                                      Arrays.toString(assertPartitionKeys),
                                      Arrays.toString(condition.getPartitionKeys()));
                throw new PreconditionException(PreconditionExceptionType.INVALID_PARTITION_KEYS,
                                                msg);
            }

        }

        if (json.has("assertSortKeys"))
        {
            String[] assertSortKeys = JsonUtils.asArray(json, "assertSortKeys");
            if (!Arrays.equals(assertSortKeys, condition.getSortKeys()))
            {
                String msg =
                        String.format("Expected=%s. Found=%s",
                                      Arrays.toString(assertSortKeys),
                                      Arrays.toString(condition.getSortKeys()));
                throw new PreconditionException(PreconditionExceptionType.INVALID_SORT_KEYS,
                                                msg);
            }

        }

        return condition;
    }
}
