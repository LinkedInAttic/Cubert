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
 * Sequentially wraps multiple blocks into a one block.
 * 
 * @author Maneesh Varshney
 * 
 */
public class GatherOperator implements TupleOperator
{
    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        throw new IllegalStateException("This operator should never be called!");
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        throw new IllegalStateException("This operator should never be called!");
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        // check that the schema of all input conditions are same
        String firstInput = preConditions.keySet().iterator().next();
        PostCondition firstCondition = preConditions.get(firstInput);
        boolean partitionKeysSame = true;

        for (String input : preConditions.keySet())
        {
            PostCondition condition = preConditions.get(input);

            if (!firstCondition.getSchema().equals(condition.getSchema()))
            {
                String msg =
                        String.format("The schemas for input block do not match."
                                              + "\n\tBlock %s: %s\n\tBlock %s: %s",
                                      firstInput,
                                      firstCondition,
                                      input,
                                      condition);

                throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA,
                                                msg);
            }

            if (!java.util.Arrays.equals(firstCondition.getPartitionKeys(),
                                         condition.getPartitionKeys()))
                partitionKeysSame = false;
        }

        return new PostCondition(firstCondition.getSchema(), partitionKeysSame
                ? firstCondition.getPartitionKeys() : null, null);
    }
}
