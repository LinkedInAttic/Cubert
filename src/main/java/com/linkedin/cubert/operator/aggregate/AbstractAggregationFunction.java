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
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.TupleUtils;

public abstract class AbstractAggregationFunction implements AggregationFunction
{
    protected int inputColumnIndex;
    protected DataType inputDataType;
    protected int outputColumnIndex;
    protected DataType outputDataType;

    protected long longAggVal;
    protected double doubleAggVal;

    protected boolean nonNullValueSeen = false;

    @Override
    public void setup(Block block, BlockSchema outputSchema, JsonNode json) throws IOException
    {
        BlockSchema inputSchema = block.getProperties().getSchema();

        String[] inputColumnNames = JsonUtils.asArray(json.get("input"));

        inputColumnIndex = inputSchema.getIndex(inputColumnNames[0]);
        inputDataType = inputSchema.getType(inputColumnIndex);

        String outputColumnName = JsonUtils.getText(json, "output");
        outputColumnIndex = outputSchema.getIndex(outputColumnName);
        outputDataType = outputSchema.getType(outputColumnIndex);

        resetState();
    }

    /**
     * NOTE: This derived method is made final to protect <code>nonNullValueSeen</code> flag.
     * To use this class effectively, override <code>resetAggregateValues</code> method to clean internal
     * datastructures.
     *
     * Alternatively, directly derive from base class.
     */
    @Override
    final public void resetState()
    {
        nonNullValueSeen = false;
        resetAggregateValues();
    }

    /**
     *
     * All derived classes need to override this method to clear internal aggregation values.
     */
    public abstract void resetAggregateValues();

    @Override
    public abstract void aggregate(Tuple input) throws IOException;

    @Override
    public void output(Tuple output) throws IOException
    {
        if (!nonNullValueSeen)
        {
            output.set(outputColumnIndex, null);
        }
        else
        {
            if (outputDataType.isReal())
                TupleUtils.setDouble(output,
                                     doubleAggVal,
                                     outputColumnIndex,
                                     outputDataType);
            else
                TupleUtils.setLong(output, longAggVal, outputColumnIndex, outputDataType);
        }

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
}
