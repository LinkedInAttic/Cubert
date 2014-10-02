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

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.TupleUtils;

public class CreateArray implements AggregationFunction
{
    public CreateArray()
    {

    }

    public BagFactory mBagFactory = BagFactory.getInstance();
    private DataBag outputBag;

    private int inputColumnIndex;
    private int outputColumnIndex;

    @Override
    public void setup(Block block, BlockSchema outputSchema, JsonNode json) throws IOException
    {
        BlockSchema inputSchema = block.getProperties().getSchema();
        String[] inputColumnNames = JsonUtils.asArray(json, "input");
        assert (inputColumnNames.length == 1);
        String inputColumnName = inputColumnNames[0];
        inputColumnIndex = inputSchema.getIndex(inputColumnName);

        outputBag = mBagFactory.newDefaultBag();

        String outputColumnName = JsonUtils.getText(json, "output");
        outputColumnIndex = outputSchema.getIndex(outputColumnName);

        resetState();
    }

    @Override
    public void resetState()
    {
        outputBag = mBagFactory.newDefaultBag();
    }

    @Override
    public void aggregate(Tuple inputTuple) throws IOException
    {
        Tuple tupleToBeAdded = (Tuple) inputTuple.get(inputColumnIndex);
        outputBag.add(TupleUtils.getDeepCopy(tupleToBeAdded));
    }

    @Override
    public void output(Tuple outputTuple) throws IOException
    {
        outputTuple.set(outputColumnIndex, outputBag);
        resetState();
    }

    @Override
    public void resetTuple(Tuple outputTuple) throws IOException
    {
        outputTuple.set(outputColumnIndex, mBagFactory.newDefaultBag());
    }

    @Override
    public BlockSchema outputSchema(BlockSchema inputSchema, JsonNode aggregateJson)
    {
        String inputColumnName = JsonUtils.asArray(aggregateJson, "input")[0];
        int inputColIndex = inputSchema.getIndex(inputColumnName);
        ColumnType inputColType = inputSchema.getColumnType(inputColIndex);

        ColumnType outColType =
                new ColumnType("ARRAY___" + inputColumnName, DataType.BAG);

        outColType.setColumnSchema(new BlockSchema(new ColumnType[] { inputColType }));

        return new BlockSchema(new ColumnType[] { outColType });
    }
}
