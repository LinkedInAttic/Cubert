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

package com.linkedin.cubert.operator.cube;

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * Builtin default implementation of a {@link CubeAggregator} that aggregates input
 * columns using a specified {@link ValueAggregator}.
 * <p>
 * Implementation notes: this object creates a long[] array for storing aggregates for the
 * value cuboids.
 * 
 * @see DefaultDupleCubeAggregator
 * @author Maneesh Varshney
 * 
 */
public class DefaultCubeAggregator implements CubeAggregator
{
    // dummyObject is used when we cannot determine the input column (e.g. COUNT() does
    // not specify an input column name)
    protected static final Object dummyObject = new Object();

    // the array to store aggregated results for the value cuboids
    protected long[] valueTable;

    // the value of the aggregate column in the input tuple
    protected Object currentValue;

    // the index of the column to aggregate in the input tuple
    protected int valueIndex;

    // the data type of the column to aggregate
    protected DataType valueType;

    // the index of the output aggregate field in the output tuple
    protected int outIndex;

    // the ValueAggregator for aggregating values
    protected ValueAggregator aggregator;

    public DefaultCubeAggregator(ValueAggregator aggregator)
    {
        this.aggregator = aggregator;
    }

    @Override
    public void setup(Block block, BlockSchema outputSchema, JsonNode json) throws IOException
    {
        BlockSchema inputSchema = block.getProperties().getSchema();

        // determine the input column name and index within the input tuple
        String inColName = null;
        if (json.has("input") && !json.get("input").isNull())
            inColName = JsonUtils.asArray(json, "input")[0];

        if (inColName != null)
            valueIndex = inputSchema.getIndex(inColName);
        else
            valueIndex = -1;

        // determine the name of output column name the index within the output tuple
        String outColName = JsonUtils.getText(json, "output");
        outIndex = outputSchema.getIndex(outColName);
    }

    @Override
    public void allocate(int size)
    {
        // allocate and initialize the long array
        valueTable = new long[size];
        for (int i = 0; i < size; i++)
            valueTable[i] = aggregator.initialValue();
    }

    @Override
    public void clear()
    {
        int size = valueTable.length;
        for (int i = 0; i < size; i++)
            valueTable[i] = aggregator.initialValue();
    }

    @Override
    public void processTuple(Tuple tuple) throws ExecException
    {
        // if we don't know the input value, use the dummyObject
        currentValue = (valueIndex == -1) ? dummyObject : tuple.get(valueIndex);
    }

    @Override
    public void aggregate(int index)
    {
        if (currentValue == null)
            return;

        valueTable[index] = aggregator.aggregate(valueTable[index], currentValue);
    }

    @Override
    public void outputTuple(Tuple outputTuple, int index) throws ExecException
    {
        outputTuple.set(outIndex, aggregator.output(valueTable[index]));
    }

    @Override
    public BlockSchema outputSchema(BlockSchema inputSchema, JsonNode json) throws PreconditionException
    {
        String str =
                String.format("%s %s",
                              aggregator.outputType(),
                              JsonUtils.getText(json, "output"));

        return new BlockSchema(str);
    }
}
