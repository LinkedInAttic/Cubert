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

package com.linkedin.cubert.operator.aggregate;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.operator.PreconditionExceptionType;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.TupleUtils;

public class CountAggregation implements AggregationFunction
{
    private int inputColumnIndex;
    private int outputColumnIndex;
    private DataType outputDataType;

    long count;
    boolean countAll = false;

    @Override
    public void setup(Block block, BlockSchema outputSchema, JsonNode json) throws IOException
    {
        String outputColumnName = JsonUtils.getText(json, "output");

        String[] inputColumnNames = JsonUtils.asArray(json.get("input"));

        // this would be the case for count all;
        if (inputColumnNames.length == 0)
            countAll = false;
        else
            inputColumnIndex =
                    block.getProperties().getSchema().getIndex(inputColumnNames[0]);

        outputColumnIndex = outputSchema.getIndex(outputColumnName);
        outputDataType = outputSchema.getType(outputColumnIndex);

        resetState();
    }

    @Override
    public void resetState()
    {
        count = 0;
    }

    @Override
    public void aggregate(Tuple inputTuple) throws IOException
    {
        if (!countAll)
        {
            Object obj = inputTuple.get(inputColumnIndex);
            if (obj == null)
                return;
        }

        count++;
    }

    @Override
    public void output(Tuple outputTuple) throws IOException
    {
        TupleUtils.setLong(outputTuple, count, outputColumnIndex, outputDataType);
        resetState();
    }

    @Override
    public void resetTuple(Tuple outputTuple) throws IOException
    {
        TupleUtils.setLong(outputTuple, 0L, outputColumnIndex, outputDataType);
    }

    @Override
    public BlockSchema outputSchema(BlockSchema inputSchema, JsonNode json) throws PreconditionException
    {
        String[] inputColNames = JsonUtils.asArray(json, "input");
        if (inputColNames.length > 1)
            throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG,
                                            "Zero or one column expected for MAX. Found: "
                                                    + JsonUtils.get(json, "input"));

        if (inputColNames.length > 0 && !inputSchema.hasIndex(inputColNames[0]))
            throw new PreconditionException(PreconditionExceptionType.COLUMN_NOT_PRESENT,
                                            inputColNames[0]);

        String outputColName = JsonUtils.getText(json, "output");

        return new BlockSchema("LONG " + outputColName);
    }
}
