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

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.operator.aggregate.AggregationFunction;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.TupleUtils;

public class TestUDAF implements AggregationFunction
{

    public TestUDAF()
    {

    }

    private int inputColumnIndex;
    private com.linkedin.cubert.block.DataType inputDataType;
    private int outputColumnIndex;
    private com.linkedin.cubert.block.DataType outputDataType;

    long longSum;
    double doubleSum;

    @Override
    public void setup(Block block, BlockSchema outputTupleSchema, JsonNode json) throws IOException
    {
        BlockSchema inputSchema = block.getProperties().getSchema();

        String inputColumnName = JsonUtils.getText(json, "input");
        inputColumnIndex = inputSchema.getIndex(inputColumnName);
        inputDataType = inputSchema.getType(inputColumnIndex);

        // FieldSchema fs =
        // outputSchema(SchemaUtils.convertFromBlockSchema(inputSchema), json);
        BlockSchema aggOutputSchema = outputSchema(inputSchema, json);
        String outputColumnName = aggOutputSchema.getName(0);

        outputColumnIndex = outputTupleSchema.getIndex(outputColumnName);
        outputDataType = outputTupleSchema.getType(outputColumnIndex);

        resetState();
    }

    @Override
    public void resetState()
    {
        longSum = 0;
        doubleSum = 0;
    }

    @Override
    public void aggregate(Tuple inputTuple) throws IOException
    {
        Number val = (Number) inputTuple.get(inputColumnIndex);

        if (inputDataType.isReal())
            doubleSum += val.doubleValue();
        else
            longSum += val.longValue();
        // if (inputDataType.isReal())
        // doubleSum +=
        // TupleUtils.getDouble(inputTuple, inputColumnIndex, inputDataType);
        // else
        // longSum += TupleUtils.getLong(inputTuple, inputColumnIndex, inputDataType);

    }

    @Override
    public void output(Tuple outputTuple) throws IOException
    {
        if (outputDataType.isReal())
            TupleUtils.setDouble(outputTuple,
                                 doubleSum,
                                 outputColumnIndex,
                                 outputDataType);
        else
            TupleUtils.setLong(outputTuple, longSum, outputColumnIndex, outputDataType);

        resetState();
    }

    @Override
    public void resetTuple(Tuple outputTuple) throws IOException
    {
        if (outputDataType.isReal())
            TupleUtils.setDouble(outputTuple, 0F, outputColumnIndex, outputDataType);
        else
            TupleUtils.setLong(outputTuple, 0L, outputColumnIndex, outputDataType);
    }

    @Override
    public BlockSchema outputSchema(BlockSchema inputSchema, JsonNode aggregateJson)
    {

        return new BlockSchema("LONG count");
    }

}
