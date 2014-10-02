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

public class BitwiseORAggregation implements AggregationFunction
{
    private int bitmap;
    private int inputColumnIndex;
    private int outputColumnIndex;

    private boolean nonNullValueSeen = false;

    @Override
    public void setup(Block block, BlockSchema outputSchema, JsonNode json) throws IOException
    {
        BlockSchema inputSchema = block.getProperties().getSchema();

        String inputColumnName = JsonUtils.asArray(json, "input")[0];
        inputColumnIndex = inputSchema.getIndex(inputColumnName);

        String outputColumnName = JsonUtils.getText(json, "output");
        outputColumnIndex = outputSchema.getIndex(outputColumnName);

        resetState();
    }

    @Override
    public void resetState()
    {
        bitmap = 0;
        nonNullValueSeen = false;
    }

    @Override
    public void aggregate(Tuple input) throws IOException
    {
        Object obj = input.get(inputColumnIndex);
        if (obj == null)
            return;

        nonNullValueSeen = true;

        int value = (Integer) input.get(inputColumnIndex);
        bitmap |= value;
    }

    @Override
    public void output(Tuple output) throws IOException
    {
        if (nonNullValueSeen)
            output.set(outputColumnIndex, bitmap);
        else
            output.set(outputColumnIndex, null);
        resetState();
    }

    @Override
    public void resetTuple(Tuple output) throws IOException
    {
        output.set(outputColumnIndex, 0);
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
        DataType inputType = inputSchema.getType(inputSchema.getIndex(inputColName));

        if (inputType != DataType.INT)
            throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA,
                                            "Expected type of column " + inputColName
                                                    + " is INT. Found: " + inputType);

        return new BlockSchema(inputType + " " + outputColName);
    }
}
