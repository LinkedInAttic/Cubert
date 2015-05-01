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
import com.linkedin.cubert.block.PivotedBlock;
import com.linkedin.cubert.utils.TupleUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;


public class DistinctOperator implements TupleOperator
{
    private PivotedBlock pivotedBlock;
    private Tuple outputTuple;
    private boolean hasMore = true;
//    private TupleCopier tupleCopier;

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props)
        throws IOException, InterruptedException
    {
        Block inputBlock = input.values().iterator().next();

        BlockSchema schema = inputBlock.getProperties().getSchema();
        pivotedBlock = new PivotedBlock(inputBlock, schema.getColumnNames());

//        tupleCopier = new TupleCopier(schema);
//        outputTuple = tupleCopier.newTuple();
        outputTuple = TupleFactory.getInstance().newTuple(schema.getNumColumns());
    }

    @Override
    public Tuple next()
        throws IOException, InterruptedException
    {
        if (!hasMore)
        {
            return null;
        }

        Tuple tuple = pivotedBlock.next();
        if (tuple == null)
        {
            return null;
        }

        // TODO: use copiers. For now using deep copy API. Will degenerate to copy if columns are shallow copiable.
        // tupleCopier.copy(tuple, outputTuple);
        TupleUtils.deepCopy(tuple, outputTuple);

        while (pivotedBlock.next() != null)
        {
            // skip these tuples
        }
        hasMore = pivotedBlock.advancePivot();

        return outputTuple;
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions, JsonNode json)
        throws PreconditionException
    {
        PostCondition condition = preConditions.values().iterator().next();
        String[] columns = condition.getSchema().getColumnNames();

        if (condition.getSortKeys() == null || condition.getSortKeys().length != columns.length)
        {
            throw new PreconditionException(PreconditionExceptionType.INVALID_SORT_KEYS, String
                .format("Columns=%s Sorted=%s", Arrays.toString(columns), Arrays.toString(condition.getSortKeys())));
        }

        return preConditions.values().iterator().next();
    }
}
