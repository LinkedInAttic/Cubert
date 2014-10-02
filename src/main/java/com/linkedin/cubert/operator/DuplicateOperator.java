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
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * 
 * Custom operator needed for SPI and PYMK. This duplicates the conns connection entry for
 * both forward and reverse direction.
 * 
 * @author Krishna Puttaswamy
 * 
 */

public class DuplicateOperator implements TupleOperator
{
    private Block block;
    private Tuple outputTuple;
    private int counter = 0;
    private int maxCounter = 0;
    private int counterColumnIndex;

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        block = input.values().iterator().next();
        maxCounter = json.get("times").getIntValue();

        int numColumns = props.getSchema().getNumColumns();
        outputTuple = TupleFactory.getInstance().newTuple(numColumns);
        counterColumnIndex = numColumns - 1;

        counter = maxCounter;
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        if (counter == maxCounter)
        {
            Tuple tuple = block.next();
            if (tuple == null)
                return null;

            for (int i = 0; i < counterColumnIndex; i++)
                outputTuple.set(i, tuple.get(i));

            counter = 0;
        }

        outputTuple.set(counterColumnIndex, counter);
        counter++;
        return outputTuple;
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        PostCondition condition = preConditions.values().iterator().next();
        String counterName = JsonUtils.getText(json, "counter");
        BlockSchema schema =
                condition.getSchema().append(new BlockSchema("int " + counterName));
        return new PostCondition(schema,
                                 condition.getPartitionKeys(),
                                 condition.getSortKeys());
    }
}
