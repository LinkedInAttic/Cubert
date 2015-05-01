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

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;

import java.io.IOException;
import java.util.Map;

import static com.linkedin.cubert.utils.JsonUtils.*;

/**
 * The map side operator for the ReduceJoin.
 *
 * @author Maneesh Varshney
 */
public class RSJoinMapOperator implements TupleOperator
{
    private Block block;
    private Tuple outputTuple;
    private int[] columnCopyMap;

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props)
            throws IOException, InterruptedException
    {
        block = input.values().iterator().next();
        BlockSchema inSchema = block.getProperties().getSchema();
        BlockSchema outSchema = props.getSchema();

        outputTuple = TupleFactory.getInstance().newTuple(outSchema.getNumColumns());

        // columnCopyMap is an array that maps input column (at index i)
        // to output column (value at index i)
        columnCopyMap = new int[inSchema.getNumColumns()];
        for (int i = 0; i < inSchema.getNumColumns(); i++)
        {
            columnCopyMap[i] = outSchema.getIndex(inSchema.getName(i));
        }

        // set the tag
        int tag = json.get("tag").getIntValue();
        // the left mapper puts the number of columns as the tag
        if (tag != 0)
            tag = inSchema.getNumColumns();

        outputTuple.set(outSchema.getIndex("___tag"), tag);
    }

    @Override
    public Tuple next() throws IOException, InterruptedException
    {
        Tuple tuple = block.next();
        if (tuple == null)
            return null;

        for (int i = 0; i < columnCopyMap.length; i++)
            outputTuple.set(columnCopyMap[i], tuple.get(i));

        return outputTuple;
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions, JsonNode json)
            throws PreconditionException
    {
        PostCondition preCondition = preConditions.values().iterator().next();
        BlockSchema schema = preCondition.getSchema();
        String[] joinKeys = asArray(json, "joinKeys");

        for (String joinKey: joinKeys)
        {
            if (!schema.hasIndex(joinKey))
                throw new PreconditionException(PreconditionExceptionType.COLUMN_NOT_PRESENT, joinKey);
        }

        return preCondition;
    }
}
