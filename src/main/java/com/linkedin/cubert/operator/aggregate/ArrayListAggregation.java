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
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.operator.PreconditionExceptionType;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * 
 * An aggregation operator to create an ArrayList of values of a field.
 * 
 * @author Krishna Puttaswamy
 * 
 */

public class ArrayListAggregation implements AggregationFunction
{
    private int inputColumnIndex;
    private int outputColumnIndex;
    private List<Object> outputList;

    @Override
    public void setup(Block block, BlockSchema outputSchema, JsonNode json) throws IOException
    {
        BlockSchema aggOutputSchema = new BlockSchema(JsonUtils.getText(json, "output"));
        String outputColumnName = aggOutputSchema.getName(0);
        outputColumnIndex = outputSchema.getIndex(outputColumnName);

        BlockSchema inputSchema = block.getProperties().getSchema();
        String inputColumnName = JsonUtils.asArray(json, "input")[0];
        inputColumnIndex = inputSchema.getIndex(inputColumnName);

        outputList = new ArrayList<Object>();
        resetState();
    }

    @Override
    public void resetState()
    {
        outputList.clear();
    }

    @Override
    public void aggregate(Tuple inputTuple) throws IOException
    {
        Object obj = inputTuple.get(inputColumnIndex);
        outputList.add(obj);
    }

    @Override
    public void output(Tuple outputTuple) throws IOException
    {
        outputTuple.set(outputColumnIndex, outputList);
    }

    @Override
    public void resetTuple(Tuple outputTuple) throws IOException
    {
        outputTuple.set(outputColumnIndex, null);
    }

    @Override
    public BlockSchema outputSchema(BlockSchema inputSchema, JsonNode json) throws PreconditionException
    {
        String[] inputColNames = JsonUtils.asArray(json, "input");
        if (inputColNames.length != 1)
            throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG,
                                            "Only one column expected for MAX. Found: "
                                                    + JsonUtils.get(json, "input"));
        // TODO
        return null;
    }

}
