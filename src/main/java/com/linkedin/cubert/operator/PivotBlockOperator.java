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

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.PivotedBlock;
import com.linkedin.cubert.block.TupleStoreBlock;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.RawTupleStore;
import com.linkedin.cubert.utils.SerializedTupleStore;
import com.linkedin.cubert.utils.TupleStore;

/**
 * Pivots a block on specified keys.
 * 
 * This is a block operator -- that is, an operator that generates multiple blocks as
 * output. The number of output blocks is equal to the number of distinct pivot keys found
 * in the input block.
 * 
 * This operator can operate either in streaming fashion (reads one tuple at a time from
 * the input source), on in bulk load manner (loads all input data in memory first). This
 * behavior is controlled by the "inMemory" boolean flag in the JSON.
 * 
 * If the data is bulk loaded in memory (when inMemory=true in JSON), this operator
 * exhibits an "auto rewind" behavior. The data can chosen to be serialized to reduce the
 * memory usage, or not serialized to have better speed.
 * 
 * Auto Rewind: In standard use, a block generate tuples (in the next() method) and
 * finally when it cannot generate any more data, it returns null. If the next() method
 * were to be called from this point on, the block will keep not returning null,
 * indicating that is no data to return. In auto-rewind case, however, the block will
 * rewind to the beginning of the buffer once it has exhausted all tuples. That is, the
 * block will first return tuples in the next() method, and when not more data is
 * available, it will return null. After returning null, this block will then rewind the
 * in-memory buffer. Therefore, if a next() call were to be made now, the first tuple will
 * be returned.
 * 
 * Note that auto-rewind is possible only when this operator bulk loads all input in
 * memory.
 * 
 * @author Maneesh Varshney
 * 
 */
public class PivotBlockOperator implements BlockOperator
{
    private Block sourceBlock;
    private BlockProperties props;
    private boolean inMemory = false;
    private boolean firstBlock = true;
    private boolean serialized = false;
    private boolean pivoted = false;

    @Override
    public void setInput(Configuration conf, Map<String, Block> input, JsonNode json) throws IOException,
            InterruptedException
    {
        Block inputBlock = input.values().iterator().next();
        this.props = props;

        String[] pivotBy = JsonUtils.asArray(json, "pivotBy");
        boolean inMemory = json.get("inMemory").getBooleanValue();

        setInput(inputBlock, pivotBy, inMemory);
    }

    public void setInput(Block block, String[] pivotBy, boolean inMemory)
    {
        if (pivotBy.length > 0)
        {
            sourceBlock = new PivotedBlock(block, pivotBy);
            pivoted = true;
        }
        else
            sourceBlock = block;
        this.inMemory = inMemory;
    }

    @Override
    public Block next() throws IOException,
            InterruptedException
    {
        if (firstBlock)
        {
            firstBlock = false;
            if (inMemory)
                return loadInMemory(sourceBlock, serialized);
            else
                return sourceBlock;
        }

        // only one block to return for non-pivoted case
        if (!pivoted)
            return null;

        if (!((PivotedBlock) sourceBlock).advancePivot())
            return null;

        if (inMemory)
            return loadInMemory(sourceBlock, serialized);
        else
            return sourceBlock;

    }

    // bulk load the data in memory, and store it in TupleStore.
    private Block loadInMemory(Block block, boolean serialize) throws IOException,
            InterruptedException
    {
        TupleStore store =
                serialized ? new SerializedTupleStore(block.getProperties().getSchema())
                        : new RawTupleStore(block.getProperties().getSchema());

        Tuple tuple;
        while ((tuple = block.next()) != null)
        {
            store.addToStore(tuple);
        }

        return new TupleStoreBlock(store, props);
    }

}
