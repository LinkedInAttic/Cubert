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

package com.linkedin.cubert.block;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.utils.JsonUtils;

/**
 * Creates tuples by merging two ByteArrayTuples.
 * 
 * This creator is needed in the reducer stage, where the Mapper key and values are merged
 * to create a single tuple.
 * 
 * @author Maneesh Varshney
 * 
 */
public class MergedTupleCreator implements TupleCreator
{
    private BlockSchema keySchema;
    private boolean valueIsNull;
    private Tuple outputTuple;
    private int[] keyFieldIndex;
    private int[] valueFieldIndex;

    @Override
    public void setup(JsonNode json) throws IOException
    {
        String[] pivotColumns = JsonUtils.asArray(json.get("pivotKeys"));

        BlockSchema fullSchema = new BlockSchema(json.get("schema"));
        keySchema = fullSchema.getSubset(pivotColumns);
        BlockSchema valueSchema = fullSchema.getComplementSubset(pivotColumns);

        keyFieldIndex = new int[keySchema.getNumColumns()];
        valueFieldIndex = new int[valueSchema.getNumColumns()];

        for (int i = 0; i < keyFieldIndex.length; i++)
        {
            keyFieldIndex[i] = fullSchema.getIndex(keySchema.getName(i));
        }

        for (int i = 0; i < valueFieldIndex.length; i++)
        {
            valueFieldIndex[i] = fullSchema.getIndex(valueSchema.getName(i));
        }

        outputTuple = TupleFactory.getInstance().newTuple(fullSchema.getNumColumns());
        valueIsNull = (valueSchema.getNumColumns() == 0);
    }

    @Override
    public Tuple create(Object key, Object value) throws IOException
    {
        Tuple keyTuple = (Tuple) key;
        for (int i = 0; i < keyTuple.size(); i++)
        {
            outputTuple.set(keyFieldIndex[i], keyTuple.get(i));
        }

        if (!valueIsNull)
        {
            Tuple valueTuple = (Tuple) value;
            for (int i = 0; i < valueTuple.size(); i++)
            {
                outputTuple.set(valueFieldIndex[i], valueTuple.get(i));
            }
        }

        return outputTuple;
    }
}
