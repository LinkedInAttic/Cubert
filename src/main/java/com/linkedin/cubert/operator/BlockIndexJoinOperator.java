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
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.Index;
import com.linkedin.cubert.utils.FileCache;
import com.linkedin.cubert.utils.JsonUtils;

public class BlockIndexJoinOperator implements TupleOperator
{
    public static final String INPUT_INDEX_FILE_PATH = "indexFilePath";
    public static final String OUTPUT_BLOCKID_NAME = "BLOCK_ID";
    public static final String INPUT_PARTITION_KEY_COLUMNS = "partitionKeys";

    BlockSchema inputColumns;

    private Block block;

    private Tuple outputTuple;
    private Tuple partitionKey;
    private int[] partitionKeyIndex;
    private Index index = null;

    @Override
    public void setInput(Map<String, Block> input, JsonNode root, BlockProperties props) throws JsonParseException,
            JsonMappingException,
            IOException
    {
        block = input.values().iterator().next();
        String[] partitionKeyColumnNames =
                JsonUtils.asArray(root, INPUT_PARTITION_KEY_COLUMNS);

        BlockSchema inputSchema = block.getProperties().getSchema();

        outputTuple =
                TupleFactory.getInstance().newTuple(inputSchema.getNumColumns() + 1);
        partitionKey =
                TupleFactory.getInstance().newTuple(partitionKeyColumnNames.length);
        partitionKeyIndex = new int[partitionKeyColumnNames.length];
        for (int i = 0; i < partitionKeyIndex.length; i++)
            partitionKeyIndex[i] = inputSchema.getIndex(partitionKeyColumnNames[i]);

        String indexName = JsonUtils.getText(root, "index");
        try
        {
            index = FileCache.getCachedIndex(indexName);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }

        if (index == null)
        {
            throw new RuntimeException("Cannot load index for ["
                    + JsonUtils.getText(root, "indexName") + "]");
        }
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        Tuple tuple = block.next();
        if (tuple == null)
            return null;

        for (int i = 0; i < partitionKeyIndex.length; i++)
            partitionKey.set(i, tuple.get(partitionKeyIndex[i]));

        long blockId = index.getBlockId(partitionKey);

        if (blockId < 0)
            throw new RuntimeException(String.format("The block id is -1 for partitionKey %s",
                                                     partitionKey.toString()));

        int i = 0;
        for (i = 0; i < tuple.size(); i++)
            outputTuple.set(i, tuple.get(i));
        outputTuple.set(i, blockId);

        return outputTuple;
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        PostCondition condition = preConditions.values().iterator().next();
        BlockSchema inputSchema = condition.getSchema();
        BlockSchema blockIdSchema = new BlockSchema("long " + OUTPUT_BLOCKID_NAME);
        BlockSchema outputSchema = inputSchema.append(blockIdSchema);

        return new PostCondition(outputSchema,
                                 condition.getPartitionKeys(),
                                 condition.getSortKeys());
    }
}
