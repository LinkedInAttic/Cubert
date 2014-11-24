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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.operator.BlockOperator;
import com.linkedin.cubert.operator.PhaseContext;
import com.linkedin.cubert.operator.PostCondition;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.utils.CommonUtils;
import com.linkedin.cubert.utils.DefaultSortAlgo;
import com.linkedin.cubert.utils.FileCache;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.SerializedTupleStore;
import com.linkedin.cubert.utils.TupleUtils;

/**
 * A BlockOperator that creates blocks.
 * 
 * @author Maneesh Varshney
 * 
 */
public class CreateBlockOperator implements BlockOperator
{

    private static class CostFunction
    {
        protected final long blockgenValue;
        protected final BlockgenType type;

        CostFunction(long blockgenValue, BlockgenType type)
        {
            this.blockgenValue = blockgenValue;
            this.type = type;
        }

        protected boolean isCostExceeded(long numTuples,
                                         long numPartitionKeys,
                                         long storeSize)

        {
            switch (type)
            {
            case BY_BLOCK_ID:
                return numPartitionKeys > 0;
            case BY_INDEX:
                return numPartitionKeys > 0;
            case BY_PARTITION_KEY:
                return numPartitionKeys >= blockgenValue;
            case BY_ROW:
                return numTuples >= blockgenValue;
            case BY_SIZE:
                return storeSize >= blockgenValue;
            case USER_DEFINED:
                break;
            default:
                break;

            }
            return false;
        }
    }

    static final class StoredBlock implements Block
    {
        private final PivotedBlock input;
        private Iterator<Tuple> iterator;
        private int numPartitionKeys;
        private final SerializedTupleStore store;
        private final BlockProperties props;

        StoredBlock(PivotedBlock input,
                    String[] partitionKeys,
                    String[] sortKeys,
                    CostFunction costFunction) throws IOException, InterruptedException
        {
            this.input = input;
            BlockSchema inputSchema = input.getProperties().getSchema();
            props = new BlockProperties("", inputSchema, input.getProperties());
            props.setBlockId(-1);

            // if no need for sorting, then use the simpler SerializedTupleStore (that
            // does not maintain offsetList)
            if (sortKeys == null)
                store = new SerializedTupleStore(inputSchema);
            else
                store = new SerializedTupleStore(inputSchema, sortKeys);

            // fetch the first tuple.. this will be the partition key
            Tuple tuple = input.next();

            if (tuple == null)
            {
                // If there is no input data, set "empty iterator"
                iterator = new Iterator<Tuple>()
                {
                    @Override
                    public boolean hasNext()
                    {
                        return false;
                    }

                    @Override
                    public Tuple next()
                    {
                        return null;
                    }

                    @Override
                    public void remove()
                    {
                    }

                };

                return;
            }
            Tuple partitionKey =
                    TupleUtils.extractTuple(tuple, inputSchema, partitionKeys);
            props.setPartitionKey(partitionKey);

            store.addToStore(tuple);

            while (true)
            {
                while ((tuple = input.next()) != null)
                    store.addToStore(tuple);

                numPartitionKeys++;

                if (costFunction.isCostExceeded(store.getNumTuples(),
                                                numPartitionKeys,
                                                store.size()))
                    break;

                if (!input.advancePivot())
                    break;

            }

            System.out.println("Store size is " + store.size());

            if (sortKeys != null)
                store.sort(new DefaultSortAlgo<Tuple>());

            iterator = store.iterator();
        }

        @Override
        public void configure(JsonNode json) throws IOException,
                InterruptedException
        {

        }

        @Override
        public Tuple next() throws IOException,
                InterruptedException
        {
            if (iterator.hasNext())
                return iterator.next();
            else
            {
                store.clear();
                return null;
            }
        }

        @Override
        public void rewind() throws IOException
        {

        }

        @Override
        public BlockProperties getProperties()
        {
            return props;
        }

    }

    static class StreamingBlock implements Block
    {
        protected BlockProperties props;
        protected final PivotedBlock input;
        private long numPartitionKeys;
        private long numTuples;
        protected Tuple firstTuple;
        private boolean isFirstTuple = true;
        private final CostFunction costFunction;

        StreamingBlock(PivotedBlock input,
                       String[] partitionKeys,
                       CostFunction costFunction) throws IOException,
                InterruptedException
        {
            this.input = input;
            this.costFunction = costFunction;

            BlockSchema inputSchema = input.getProperties().getSchema();
            props = new BlockProperties("", inputSchema, input.getProperties());
            props.setBlockId(-1);

            firstTuple = input.next();
            if (firstTuple == null)
            {
                return;
            }
            Tuple partitionKey =
                    TupleUtils.extractTuple(firstTuple, inputSchema, partitionKeys);
            props.setPartitionKey(partitionKey);
            numPartitionKeys++;
        }

        @Override
        public void configure(JsonNode json) throws IOException
        {

        }

        @Override
        public Tuple next() throws IOException,
                InterruptedException
        {
            if (isFirstTuple)
            {
                isFirstTuple = false;

                if (firstTuple == null)
                    return null;

                numTuples++;
                return firstTuple;
            }

            Tuple tuple = input.next();
            if (tuple != null)
            {
                numTuples++;
                return tuple;
            }

            if (costFunction.isCostExceeded(numTuples, numPartitionKeys, 0))
                return null;

            if (!input.advancePivot())
                return null;

            numPartitionKeys++;
            return next();
        }

        @Override
        public void rewind() throws IOException
        {

        }

        @Override
        public BlockProperties getProperties()
        {
            return props;
        }

    }

    private static class ByIndexBlock extends StreamingBlock
    {
        private Index index;
        private int[] colIndex;
        private Tuple outputTuple;

        ByIndexBlock(PivotedBlock input, String[] partitionKeys, CostFunction costFunction) throws IOException,
                InterruptedException
        {
            super(input, partitionKeys, costFunction);

            BlockSchema inputSchema = input.getProperties().getSchema();
            BlockSchema outputSchema =
                    inputSchema.getComplementSubset(new String[] { "BLOCK_ID" });

            colIndex = new int[outputSchema.getNumColumns()];
            for (int i = 0; i < colIndex.length; i++)
                colIndex[i] = inputSchema.getIndex(outputSchema.getName(i));

            outputTuple = TupleFactory.getInstance().newTuple(colIndex.length);

            props = new BlockProperties("", outputSchema, input.getProperties());
            props.setBlockId(-1);
        }

        @Override
        public void configure(JsonNode json) throws IOException
        {
            String indexName = JsonUtils.getText(json, "index");
            try
            {
                index = FileCache.getCachedIndex(indexName);
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException(e);
            }

            if (firstTuple != null)
            {
                int blockIdIndex = input.getProperties().getSchema().getIndex("BLOCK_ID");
                configure((Long) firstTuple.get(blockIdIndex));
            }
        }

        @Override
        public Tuple next() throws IOException,
                InterruptedException
        {
            Tuple out = super.next();
            if (out == null)
                return out;

            for (int i = 0; i < colIndex.length; i++)
                outputTuple.set(i, out.get(colIndex[i]));

            return outputTuple;
        }

        /**
         * This configure method is required in cases where empty blocks need to be
         * created based on their corresponding counterparts in the parent relation.
         * 
         * @param blockId
         *            block ID to be set
         */
        public void configure(long blockId)
        {
            Tuple partitionKey = index.getEntry(blockId).getKey();
            props.setPartitionKey(partitionKey);
            props.setBlockId(blockId);
        }

    }

    private JsonNode inputJson;
    private PivotedBlock inputBlock;
    private BlockgenType blockgenType;
    private long blockgenValue;
    private String[] partitionKeys;
    private String[] sortKeys;

    private final Set<Long> relevantBlockIds = new HashSet<Long>();
    private Tuple blockIdTuple = TupleFactory.getInstance().newTuple(1);
    private int reducerId = -1;
    private int nReducers = -1;
    boolean firstBlock = true;

    @Override
    public void setInput(Configuration conf, Map<String, Block> input, JsonNode json) throws IOException,
            InterruptedException
    {
        if (input.size() == 0)
            throw new IllegalArgumentException("No input block is provided");

        if (input.size() != 1)
            throw new IllegalArgumentException("This operator operates on only one input block. ("
                    + input.size() + " provided)");

        this.inputJson = json;
        Block block = input.values().iterator().next();

        if (block == null)
            throw new IllegalArgumentException("The specified block for ["
                    + input.keySet().iterator().next() + "] is null");

        blockgenType =
                BlockgenType.valueOf(JsonUtils.getText(json, "blockgenType")
                                              .toUpperCase());
        if (json.has("blockgenValue"))
        {
            blockgenValue = json.get("blockgenValue").getLongValue();
        }
        partitionKeys = JsonUtils.asArray(json.get("partitionKeys"));
        // if (json.get("originalPartitionKeys") != null)
        // originalPartitionKeys = JsonUtils.asArray(json.get("originalPartitionKeys"));

        if (json.has("pivotKeys"))
            sortKeys = JsonUtils.asArray(json, "pivotKeys");

        // if sort keys are prefix of partitionkeys. or vice versa, there is no need to
        // sort the block
        if (CommonUtils.isPrefix(partitionKeys, sortKeys)
                || CommonUtils.isPrefix(sortKeys, partitionKeys))
            sortKeys = null;

        inputBlock = new PivotedBlock(block, partitionKeys);

        if (blockgenType == BlockgenType.BY_INDEX)
        {
            if (PhaseContext.isMapper())
            {
                throw new RuntimeException("Expecting Reduce Context while performing LOAD BLOCK");
            }
            nReducers = conf.getInt("mapred.reduce.tasks", -1);
            if (nReducers < 0)
                throw new RuntimeException("Unable to determine number of reducers.");
            reducerId =
                    PhaseContext.getRedContext().getTaskAttemptID().getTaskID().getId();
            retrieveRelevantBlockIds(json);
        }
    }

    private void retrieveRelevantBlockIds(JsonNode json) throws IOException
    {
        final Index index;
        try
        {
            String indexName = JsonUtils.getText(json, "index");
            index = FileCache.getCachedIndex(indexName);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }

        for (Long blockId : index.getAllBlockIds())
        {
            if (isRelevant(blockId))
            {
                relevantBlockIds.add(blockId);
            }
        }
    }

    /**
     * The relevance check is based on the fact that INDEX based join happens by shuffling
     * data on the BLOCK_ID.
     * 
     * @param blockId
     *            blockId
     * @return if the block is relevant to the current reducer
     */
    private boolean isRelevant(Long blockId)
    {
        try
        {
            /* Reusing the blockIdTuple object */
            blockIdTuple.set(0, blockId);
        }
        catch (ExecException e)
        {
            e.printStackTrace();
        }

        /* Is this reducer supposed to see this blockId */
        final int hashCode = BlockUtils.getBlockId(blockIdTuple);
        return hashCode % nReducers == reducerId;
    }

    @Override
    public Block next() throws IOException,
            InterruptedException
    {
        if (!firstBlock && !inputBlock.advancePivot())
        {
            /*
             * For blockgen by INDEX cases. Create empty blocks for all block IDs relevant
             * to this reducer
             */
            if (blockgenType == BlockgenType.BY_INDEX && relevantBlockIds.size() > 0)
            {
                final Long nextBlockId = relevantBlockIds.iterator().next();

                final ByIndexBlock nextBlock = (ByIndexBlock) createBlock();
                nextBlock.configure(inputJson);
                nextBlock.configure(nextBlockId);
                relevantBlockIds.remove(nextBlockId);

                return nextBlock;
            }
            return null;
        }

        final Block nextBlock = createBlock();
        nextBlock.configure(inputJson);
        /**
         * The following if check applies to a Block GEN by INDEX case where a reducer
         * doesn't receive any data. While there are relevant block ids for which it is
         * supposed to generate empty blocks. In this case First Block is true and
         * therefore the configure(nextBlockId) doesn't happen.
         * 
         * For ByIndexBlock Only The configure method sets the blockId and partitionKey
         * (using the index)
         */
        if (firstBlock && relevantBlockIds.size() > 0
                && nextBlock instanceof ByIndexBlock
                && ((ByIndexBlock) nextBlock).firstTuple == null)
        {
            final Long nextBlockId = relevantBlockIds.iterator().next();
            ((ByIndexBlock) nextBlock).configure(nextBlockId);
        }
        relevantBlockIds.remove(nextBlock.getProperties().getBlockId());

        if (firstBlock)
            firstBlock = false;

        return nextBlock;
    }

    private Block createBlock() throws IOException,
            InterruptedException
    {
        CostFunction costFunction = new CostFunction(blockgenValue, blockgenType);

        switch (blockgenType)
        {
        case BY_INDEX:
            return new ByIndexBlock(inputBlock, partitionKeys, costFunction);
        case BY_PARTITION_KEY:
            if (sortKeys == null)
                return new StreamingBlock(inputBlock, partitionKeys, costFunction);
            else
                return new StoredBlock(inputBlock, partitionKeys, sortKeys, costFunction);
        case BY_ROW:
            if (sortKeys == null)
                return new StreamingBlock(inputBlock, partitionKeys, costFunction);
            else
                return new StoredBlock(inputBlock, partitionKeys, sortKeys, costFunction);
        case BY_BLOCK_ID:
            return new ByIndexBlock(inputBlock, partitionKeys, costFunction);
        case BY_SIZE:
            return new StoredBlock(inputBlock, partitionKeys, sortKeys, costFunction);
        case USER_DEFINED:
            break;
        default:
            break;
        }

        return null;
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        // TODO Auto-generated method stub
        return null;
    }
}
