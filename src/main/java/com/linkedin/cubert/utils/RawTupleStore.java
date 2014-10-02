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

package com.linkedin.cubert.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.linkedin.cubert.block.BlockSchema;

/**
 * A tuple store for keeping PigTuples in memory in the raw format. In contrast to the
 * SerializedTupleStore, this simply keeps the objects thus it would create many objects
 * but saves the overhead for deserializing the tuples. An ideal workload for
 * RawTupleStore would be the block would be rewinded for many times, while the block is
 * relatively small, such as mesh join with many small sub blocks.
 */

public class RawTupleStore implements TupleStore
{
    private List<Tuple> tuples;
    private boolean shallowCopy;

    public RawTupleStore(BlockSchema schema)
    {
        this(schema, 1000);
    }

    public RawTupleStore(BlockSchema schema, int capacity)
    {
        shallowCopy = schema.allFieldsAllowShallowCopy();
        tuples = new ArrayList<Tuple>(capacity);
    }

    @Override
    public void addToStore(Tuple tuple) throws IOException
    {
        Tuple copiedTuple = TupleFactory.getInstance().newTuple(tuple.size());
        if (shallowCopy)
            TupleUtils.copy(tuple, copiedTuple);
        else
            TupleUtils.deepCopy(tuple, copiedTuple);
        tuples.add(copiedTuple);
    }

    @Override
    public void clear()
    {
        tuples = null;

        long before = Runtime.getRuntime().freeMemory();
        System.gc();
        long after = Runtime.getRuntime().freeMemory();
        print.f("Memory. Before=%d After=%d. Diff=%d", before, after, after - before);

    }

    @Override
    public Iterator<Tuple> iterator()
    {
        return tuples.iterator();
    }

    @Override
    public int getNumTuples()
    {
        return tuples.size();
    }
}
