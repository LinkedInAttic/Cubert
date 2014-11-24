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

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.Index;
import com.linkedin.cubert.block.IndexEntry;
import com.linkedin.cubert.block.TupleComparator;
import com.linkedin.cubert.utils.FileCache;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.TupleUtils;

/**
 * Validate operator
 * 
 * Used for debugging to validate the correctness of a block generation output
 * 
 * @author Suvodeep Pyne
 */
public class ValidateOperator implements TupleOperator
{
    private Block block;
    private Index index;
    private String[] partitionKeys, sortKeys;

    private TupleComparator comparator, partitionKeyComparator;
    private Tuple prev, start, end;
    private boolean first = true;
    private int hashPartitionId = -1;

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        block = input.values().iterator().next();
        BlockSchema inputSchema = block.getProperties().getSchema();

        partitionKeys = JsonUtils.asArray(json.get("partitionKeys"));
        final JsonNode pivotKeys = json.get("pivotKeys");
        sortKeys = pivotKeys == null ? null : JsonUtils.asArray(pivotKeys);
        prev = TupleFactory.getInstance().newTuple(inputSchema.getNumColumns());

        try
        {
            String indexName = JsonUtils.getText(json, "index");
            index = FileCache.getCachedIndex(indexName);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }

        if (index == null)
        {
            throw new RuntimeException("Cannot load index for ["
                    + JsonUtils.getText(json, "indexName") + "]");
        }
        if (getBlockId() == -1)
        {
            throw new RuntimeException("Block ID is -1. Block Creation pending / Data loaded from AVRO");
        }

        IndexEntry startEntry = index.getEntry(getBlockId());
        IndexEntry endEntry = index.getNextEntry(getBlockId());

        final BlockSchema partitionKeysSchema = inputSchema.getSubset(partitionKeys);
        partitionKeyComparator = new TupleComparator(partitionKeysSchema, partitionKeys);
        comparator =
                new TupleComparator(getSchema(), sortKeys == null ? partitionKeys
                        : sortKeys);
        start = startEntry.getKey();
        end = endEntry == null ? null : endEntry.getKey();
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        Tuple next = block.next();

        /* Exhausted the pipeline. Return */
        if (next == null)
            return null;

        Tuple partitionSubset = TupleUtils.extractTuple(next, getSchema(), partitionKeys);
        /**
         * The Reducer ID is computed as hashcode(key) % number of partitions. Validate
         * that all tuples in the blockk should hold the same value.
         */
        if (!first && hashPartitionId != index.getReducerId(partitionSubset))
        {
            throw new RuntimeException("VALIDATE Failed: hashPartitionId doesn't match for Tuple: "
                    + next);
        }
        /**
         * Validate that the block ID obtained from the block matches the one retrieved
         * from the Index
         */
        if (index.getBlockId(partitionSubset) != getBlockId())
        {
            throw new RuntimeException("VALIDATE Failed: Index entry for Tuple" + next
                    + " is inconsistent with Block: " + getBlockId());
        }
        /**
         * Validate that the tuples are actually sorted according to Tuple Comparator
         */
        if (!first && comparator.compare(prev, next) > 0)
        {
            throw new RuntimeException("VALIDATE Failed: Tuples sorted incorrectly. blockId: "
                    + getBlockId());
        }
        /**
         * Validate that all keys in the block lie within the start and end boundaries.
         * Note that the last block in the reducer doesn't know its boundary key in which
         * case end is null
         */
        if (partitionKeyComparator.compare(start, partitionSubset) > 0
                || (end != null && partitionKeyComparator.compare(end, partitionSubset) < 0))
        {
            throw new RuntimeException("VALIDATE Failed: Tuple" + end
                    + " not within bounds");
        }

        if (first)
        {
            /* Update the reducer ID once. */
            hashPartitionId = index.getReducerId(partitionSubset);
            first = false;
        }
        TupleUtils.copy(next, prev);
        return next;
    }

    public long getBlockId()
    {
        return block.getProperties().getBlockId();
    }

    public Tuple getPartitionKey()
    {
        return block.getProperties().getPartitionKey();
    }

    public BlockSchema getSchema()
    {
        return block.getProperties().getSchema();
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        return preConditions.values().iterator().next();
    }
}
