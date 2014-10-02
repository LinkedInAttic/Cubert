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
import java.util.Arrays;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.operator.PreconditionExceptionType;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.TupleUtils;

public class CountDistinctAggregation implements AggregationFunction
{
    private int inputColumnIndex;
    private int outputColumnIndex;
    private DataType outputDataType;

    Object measureValue = null;
    long countDistinctValue = 0L;

    @Override
    public void setup(Block block, BlockSchema outputSchema, JsonNode json) throws IOException
    {
        BlockSchema inputSchema = block.getProperties().getSchema();

        String[] inputColumns = JsonUtils.asArray(json.get("input"));
        if (inputColumns.length > 1)
            throw new RuntimeException("Count distinct on multiple measure columns : "
                    + Arrays.toString(inputColumns));

        String inputColumnName = inputColumns[0];

        inputColumnIndex = inputSchema.getIndex(inputColumnName);

        String outputColumnName = JsonUtils.getText(json, "output");
        outputColumnIndex = outputSchema.getIndex(outputColumnName);
        outputDataType = outputSchema.getType(outputColumnIndex);
        System.out.println("output column = " + outputColumnName
                + " outputColumnIndex = " + outputColumnIndex);

        resetState();
    }

    @Override
    public void resetState()
    {

        measureValue = null;
        countDistinctValue = 0L;
    }

    @Override
    public void aggregate(Tuple inputTuple) throws IOException
    {
        Object val = inputTuple.get(inputColumnIndex);

        // skip 'null' value measures
        if (val == null)
            return;

        // System.out.println("count distinct processing tuple " + inputTuple.toString());
        if (measureValue == null || !measureValue.equals(val))
        {
            measureValue = val;
            countDistinctValue++;
        }
    }

    @Override
    public void output(Tuple outputTuple) throws IOException
    {

        TupleUtils.setLong(outputTuple,
                           countDistinctValue,
                           outputColumnIndex,
                           outputDataType);

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
        if (inputColNames.length != 1)
            throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG,
                                            "Only one column expected for BITWISE_OR. Found: "
                                                    + JsonUtils.get(json, "input"));

        String inputColName = inputColNames[0];
        if (!inputSchema.hasIndex(inputColName))
            throw new PreconditionException(PreconditionExceptionType.COLUMN_NOT_PRESENT,
                                            inputColName);

        String outputColName = JsonUtils.getText(json, "output");

        return new BlockSchema("LONG " + outputColName);
    }
}
