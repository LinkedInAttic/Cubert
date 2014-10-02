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

package com.linkedin.cubert.io.rubix;

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.BlockWriter;
import com.linkedin.cubert.block.CommonContext;
import com.linkedin.cubert.operator.PhaseContext;

/**
 * Writes a block in the Rubix file format.
 * 
 * @author Maneesh Varshney
 * 
 */
public class RubixBlockWriter implements BlockWriter
{
    private BlockSchema outputSchema;
    private static long blockCount = 0;

    @Override
    public void configure(JsonNode json) throws JsonParseException,
            JsonMappingException,
            IOException
    {
        outputSchema = new BlockSchema(json.get("schema"));
    }

    @Override
    public void write(Block block, CommonContext context) throws IOException,
            InterruptedException
    {
        Tuple outputTuple =
                TupleFactory.getInstance().newTuple(outputSchema.getNumColumns());
        int[] outputFieldIndex = new int[outputSchema.getNumColumns()];

        for (int i = 0; i < outputFieldIndex.length; i++)
        {
            outputFieldIndex[i] =
                    block.getProperties().getSchema().getIndex(outputSchema.getName(i));
        }

        Tuple partitionKey = block.getProperties().getPartitionKey();
        if (partitionKey == null)
        {
            /**
             * This is a case where the block has no data and no partition key
             * information. In this case: Generate an empty block with an empty partition
             * key. This ensures that reducers create at least 1 block.
             */
            partitionKey = TupleFactory.getInstance().newTuple(0);
        }

        long blockId = block.getProperties().getBlockId();
        if (blockId < 0)
        {
            blockId = createBlockId();
            blockCount++;
        }
        Tuple compositeKey = TupleFactory.getInstance().newTuple(2);
        compositeKey.set(0, partitionKey);
        compositeKey.set(1, blockId);

        // Write the first tuple with key
        Tuple tuple = block.next();
        if (tuple == null) // i.e. if the block is empty
        {
            context.write(compositeKey, null);
            return;
        }
        projectColumns(tuple, outputTuple, outputFieldIndex);
        context.write(compositeKey, outputTuple);

        // write the remaining tuples with null key
        while ((tuple = block.next()) != null)
        {
            projectColumns(tuple, outputTuple, outputFieldIndex);
            context.write(null, outputTuple);
        }
    }

    private void projectColumns(Tuple input, Tuple output, int[] fieldIndex) throws ExecException
    {
        for (int i = 0; i < fieldIndex.length; i++)
        {
            output.set(i, input.get(fieldIndex[i]));
        }
    }

    private long createBlockId()
    {
        long reducerId =
                PhaseContext.getRedContext().getTaskAttemptID().getTaskID().getId();
        return (reducerId << 32) | blockCount;
    }
}
