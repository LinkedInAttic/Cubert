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

import com.linkedin.cubert.utils.TupleUtils;

/**
 * Provides pivoting on a block.
 * 
 * The data can be iterated simply via the next() method, or in pivot key aware fashion
 * via the nextPivoted() method.
 * 
 * @author Maneesh Varshney
 * 
 */
public class PivotedBlock implements Block
{
    private final Block block;
    private Tuple current = null;
    private Tuple prev;
    private boolean firstTupleInAdvance = true;
    private boolean currentPivotExhausted = false;
    private final TupleComparator comparator;

    public PivotedBlock(Block block, String[] pivotColumns)
    {
        this.block = block;

        if (block == null)
            throw new IllegalArgumentException("Input block is null");

        if (pivotColumns == null || pivotColumns.length == 0)
            throw new IllegalArgumentException("Pivot columns are not specified");

        prev =
                TupleFactory.getInstance().newTuple(block.getProperties()
                                                         .getSchema()
                                                         .getNumColumns());

        comparator = new TupleComparator(block.getProperties().getSchema(), pivotColumns);
    }

    @Override
    public void configure(JsonNode json)
    {

    }

    /*
     * This method currently does not handle:
     * 
     * Current the "advance" happens across all the configured pivot keys. This method
     * currently does not support advancing over subset of pivot keys.
     */
    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        if (currentPivotExhausted)
            return null;

        // do not read, if this is first time in advance, and there is previous saved
        // entry already
        if (!(firstTupleInAdvance && current != null))
        {
            current = block.next();
        }

        if (current == null)
        {
            currentPivotExhausted = true;
            return null;
        }

        if (!firstTupleInAdvance)
        {
            // print.f("comparing %s %s CMP=%d",
            // prev,
            // current,
            // comparator.compare(prev.getBytes(false), current.getBytes(false)));
            if (comparator.compare(prev, current) != 0)
            {
                currentPivotExhausted = true;
                return null;
            }
        }

        firstTupleInAdvance = false;

        TupleUtils.copy(current, prev);

        return current;
    }

    @Override
    public void rewind() throws IOException
    {
        block.rewind();
    }

    public boolean advancePivot() throws IOException,
            InterruptedException
    {
        // Make sure the current advance is exhausted first.
        while (!currentPivotExhausted)
            next();

        firstTupleInAdvance = true;
        currentPivotExhausted = false;
        return current != null;
    }

    @Override
    public BlockProperties getProperties()
    {
        return block.getProperties();
    }

}
