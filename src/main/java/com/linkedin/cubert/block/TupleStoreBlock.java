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
import java.util.Iterator;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.utils.TupleStore;

/**
 * Provides a block interface for an underlying TupleStore, which can be
 * SerializedTupleStore or RawTupleStore. This block exhibits the "Auto Rewind" behavior.
 * 
 * In standard use, a block generate tuples (in the next() method) and finally when it
 * cannot generate any more data, it returns null. If the next() method were to be called
 * from this point on, the block will keep not returning null, indicating that is no data
 * to return.
 * 
 * In auto-rewind case, however, the block will rewind to the beginning of the buffer once
 * it has exhausted all tuples. That is, the block will first return tuples in the next()
 * method, and when not more data is available, it will return null. After returning null,
 * this block will then rewind the in-memory buffer. Therefore, if a next() call were to
 * be made now, the first tuple will be returned.
 * 
 * @author Maneesh Varshney
 * 
 */
public class TupleStoreBlock implements Block
{
    private final TupleStore store;
    private final BlockProperties props;
    private Iterator<Tuple> iterator;

    public TupleStoreBlock(TupleStore store, BlockProperties props)
    {
        this.store = store;
        this.props = props;
        this.iterator = store.iterator();
    }

    @Override
    public BlockProperties getProperties()
    {
        return props;
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
        if (!iterator.hasNext())
        {
            // "rewinding" the iterator here!!
            iterator = store.iterator();
            return null;
        }

        return iterator.next();
    }

    @Override
    public void rewind() throws IOException
    {
        iterator = store.iterator();

    }

}
