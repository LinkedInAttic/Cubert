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
import java.util.Arrays;
import java.util.Map;

import com.linkedin.cubert.block.RowPivotedBlock;
import com.linkedin.cubert.utils.CommonUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.PivotedBlock;
import com.linkedin.cubert.block.TupleStoreBlock;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.RawTupleStore;
import com.linkedin.cubert.utils.SerializedTupleStore;
import com.linkedin.cubert.utils.TupleStore;

/**
 * Pivots a block on specified keys.
 * <p/>
 * This is a block operator -- that is, an operator that generates multiple blocks as
 * output. The number of output blocks is equal to the number of distinct pivot keys found
 * in the input block.
 * <p/>
 * This operator can operate either in streaming fashion (reads one tuple at a time from
 * the input source), on in bulk load manner (loads all input data in memory first). This
 * behavior is controlled by the "inMemory" boolean flag in the JSON.
 * <p/>
 * If the data is bulk loaded in memory (when inMemory=true in JSON), this operator
 * exhibits an "auto rewind" behavior. The data can chosen to be serialized to reduce the
 * memory usage, or not serialized to have better speed.
 * <p/>
 * Auto Rewind: In standard use, a block generate tuples (in the next() method) and
 * finally when it cannot generate any more data, it returns null. If the next() method
 * were to be called from this point on, the block will keep not returning null,
 * indicating that is no data to return. In auto-rewind case, however, the block will
 * rewind to the beginning of the buffer once it has exhausted all tuples. That is, the
 * block will first return tuples in the next() method, and when not more data is
 * available, it will return null. After returning null, this block will then rewind the
 * in-memory buffer. Therefore, if a next() call were to be made now, the first tuple will
 * be returned.
 * <p/>
 * Note that auto-rewind is possible only when this operator bulk loads all input in
 * memory.
 *
 * @author Maneesh Varshney
 */
public class PivotBlockOperator implements BlockOperator
{
    private Block sourceBlock;
    private boolean inMemory = false;
    private boolean firstBlock = true;
    private boolean serialized = false;
    private boolean pivoted = false;
    private boolean byRow = false;

    @Override
    public void setInput(Configuration conf, Map<String, Block> input, JsonNode json)
            throws IOException, InterruptedException
    {
        Block inputBlock = input.values().iterator().next();

        String[] pivotBy = JsonUtils.asArray(json, "pivotBy");
        boolean inMemory = json.get("inMemory").getBooleanValue();

        long pivotRowCount = 0L;
        if (json.has("pivotType") && JsonUtils.getText(json, "pivotType").equalsIgnoreCase("ROW"))
        {
            byRow = true;
            pivotRowCount = Long.parseLong(JsonUtils.getText(json, "pivotValue"));
        }

        setInput(inputBlock, pivotBy, inMemory, pivotRowCount);
    }

    public void setInput(Block block, String[] pivotBy, boolean inMemory) throws IOException, InterruptedException
    {
        setInput(block, pivotBy, inMemory, 0L);
    }

    public void setInput(Block block, String[] pivotBy, boolean inMemory, long pivotRowCount)
            throws IOException, InterruptedException
    {


        if (byRow)
        {
            sourceBlock = new RowPivotedBlock(block, pivotRowCount);
            pivoted = true;
        }
        else if (pivotBy.length > 0)
        {
            sourceBlock = new PivotedBlock(block, pivotBy);
            pivoted = true;
        }
        else
        {
            sourceBlock = block;
        }

        this.inMemory = inMemory;
    }

    @Override
    public Block next() throws IOException, InterruptedException
    {
        if (firstBlock)
        {
            firstBlock = false;
            if (inMemory)
            {
                return loadInMemory(sourceBlock);
            }
            else
            {
                return sourceBlock;
            }
        }

        // only one block to return for non-pivoted case
        if (!pivoted)
        {
            return null;
        }

        if (byRow)
        {
            if (!((RowPivotedBlock) sourceBlock).advancePivot())
            {
                return null;
            }
        }
        else if (!((PivotedBlock) sourceBlock).advancePivot())
        {
            return null;
        }

        if (inMemory)
        {
            return loadInMemory(sourceBlock);
        }
        else
        {
            return sourceBlock;
        }

    }

    // bulk load the data in memory, and store it in TupleStore.
    private Block loadInMemory(Block block) throws IOException, InterruptedException
    {
        TupleStore store = serialized ? new SerializedTupleStore(block.getProperties().getSchema()) : new RawTupleStore(
                block.getProperties().getSchema());

        Tuple tuple;
        while ((tuple = block.next()) != null)
        {
            store.addToStore(tuple);
        }

        return new TupleStoreBlock(store, block.getProperties());
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions, JsonNode json)
            throws PreconditionException
    {
        boolean inMemory = json.get("inMemory").getBooleanValue();
        boolean byRow = false;
        if (json.has("pivotType") && JsonUtils.getText(json, "pivotType").equalsIgnoreCase("ROW"))
        {
            byRow = true;
        }

        // Currently we don't allow pivoting BY ROW that is not IN MEMORY
        if (byRow && !inMemory)
        {
            throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG,
                                            "PIVOT BY ROW must be used in conjunction with IN MEMORY");
        }

        PostCondition preCondition = preConditions.values().iterator().next();
        String[] pivotBy = JsonUtils.asArray(json, "pivotBy");
        if (pivotBy != null)
        {
            if (!CommonUtils.isPrefix(preCondition.getSortKeys(), pivotBy))
            {
                throw new PreconditionException(PreconditionExceptionType.INVALID_SORT_KEYS);
            }
            String[] sortKeys =
                    Arrays.copyOfRange(preCondition.getSortKeys(), pivotBy.length, preCondition.getSortKeys().length);

            return new PostCondition(preCondition.getSchema(), preCondition.getPartitionKeys(), sortKeys, pivotBy);
        }
        else
        {
            return preCondition;
        }
    }

}
