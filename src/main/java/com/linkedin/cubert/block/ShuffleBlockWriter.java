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
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import com.linkedin.cubert.utils.JsonUtils;

/**
 * Writes a block in the SHUFFLE format.
 * 
 * This creates divides the input tuple into key tuple and value tuple. The key tuple
 * consists of pivoted columns, while the value tuple consists of remaining columns.
 * 
 * @author Maneesh Varshney
 * 
 */
public class ShuffleBlockWriter implements BlockWriter
{

    private JsonNode json;

    @Override
    public void configure(JsonNode json) throws JsonParseException,
            JsonMappingException,
            IOException
    {
        this.json = json;
    }

    @Override
    public void write(Block block, CommonContext context) throws IOException,
            InterruptedException
    {

        String[] pivotColumns = JsonUtils.asArray(json.get("pivotKeys"));

        BlockSchema inputSchema = block.getProperties().getSchema();
        BlockSchema outputSchema = new BlockSchema(json.get("schema"));
        BlockSchema keySchema = outputSchema.getSubset(pivotColumns);
        BlockSchema valueSchema = outputSchema.getComplementSubset(pivotColumns);

        int[] keyFieldIndex = new int[keySchema.getNumColumns()];
        int[] valueFieldIndex = new int[valueSchema.getNumColumns()];

        for (int i = 0; i < keyFieldIndex.length; i++)
        {
            keyFieldIndex[i] = inputSchema.getIndex(keySchema.getName(i));
        }

        for (int i = 0; i < valueFieldIndex.length; i++)
        {
            valueFieldIndex[i] = inputSchema.getIndex(valueSchema.getName(i));
        }

        Tuple keyTuple = TupleFactory.getInstance().newTuple(keySchema.getNumColumns());
        Tuple valueTuple =
                TupleFactory.getInstance().newTuple(valueSchema.getNumColumns());

        Tuple tuple;
        while ((tuple = block.next()) != null)
        {

            for (int i = 0; i < keyFieldIndex.length; i++)
            {
                Object val = tuple.get(keyFieldIndex[i]);
                keyTuple.set(i, val);
            }

            for (int i = 0; i < valueFieldIndex.length; i++)
            {
                Object val = tuple.get(valueFieldIndex[i]);
                valueTuple.set(i, val);
            }

            context.write(keyTuple, valueTuple);
        }

    }
}
