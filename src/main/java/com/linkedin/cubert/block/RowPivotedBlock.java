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

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import java.io.IOException;

/**
 * Creates subblocks pivoted by the number of rows.
 * <p/>
 * This class does not clone any tuple, and it works correctly if the source block is reusing tuples.
 *
 * @author Maneesh Varshney
 */
public class RowPivotedBlock implements Block
{
    private final Block block;
    private final long rowCount;
    private long remaining;
    private Tuple firstTuple = null;
    private boolean finished = false;

    public RowPivotedBlock(Block block, long rowCount) throws IOException, InterruptedException
    {
        this.block = block;
        this.rowCount = rowCount;

        this.remaining = rowCount;
        this.firstTuple = block.next();
    }

    @Override
    public void configure(JsonNode json) throws IOException, InterruptedException
    {

    }

    @Override
    public BlockProperties getProperties()
    {
        return block.getProperties();
    }

    public Tuple next() throws IOException, InterruptedException
    {
        // if we have generated the specified count of tuple, this block is finished.
        if (remaining == 0)
        {
            return null;
        }

        // get the next tuple
        Tuple tuple;

        // there may be the first tuple that is "cached" by the advancePivot()
        // if so, use it instead of fetching the tuple from the source block
        if (firstTuple != null)
        {
            tuple = firstTuple;
            firstTuple = null;
        }
        else
        {
            tuple = block.next();
        }

        // if the source block is depleted, make a note of it. This is needed in the
        // advancePivot()
        if (tuple == null)
        {
            finished = true;
            return null;
        }

        remaining--;

        return tuple;
    }

    @Override
    public void rewind() throws IOException
    {
        block.rewind();
    }

    public boolean advancePivot() throws IOException, InterruptedException
    {
        // if the source block is empty, there is no more advancing
        if (finished)
        {
            return false;
        }

        // fetch the "cache" the next tuple
        firstTuple = block.next();

        // if there is nothing, then we are done
        if (firstTuple == null)
        {
            return false;
        }

        // reset the internal state
        remaining = rowCount;

        return true;
    }
}
