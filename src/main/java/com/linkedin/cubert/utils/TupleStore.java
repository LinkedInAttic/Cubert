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
import java.util.Iterator;

import org.apache.pig.data.Tuple;

import com.linkedin.cubert.block.BlockSchema;

/**
 * The TupleStore simply stores tuples in memory.
 * 
 * This is essential for example in operations like JOINS etc where quick fetch of tuples
 * is necessary
 */
public interface TupleStore extends Iterable<Tuple>
{
    /**
     * Add a new tuple to the store
     * 
     * @param tuple
     *            the Tuple object
     * @throws IOException
     */
    public void addToStore(Tuple tuple) throws IOException;

    /**
     * Empty the store. Note: Loses all information
     */
    public void clear();

    /**
     * Iterate through the entire store
     * 
     * @return an iterator
     */
    @Override
    public Iterator<Tuple> iterator();

    /**
     * Get a tuple from the store. This method is for random data access
     * 
     * 
     * @param reuse
     *            provide a tuple to reuse instead of creating a new tuple on every call.
     *            This allows object reuse. passing null implies new Tuple creation. NOTE:
     *            reuse DOES NOT GUARANTEE that it will always use the reuse object. It
     *            may return a different object if that is readily available. This is
     *            essentially for performance reasons to avoid an additional copy of data
     *            to the reuse tuple
     * @param index
     *            position or offset in the store
     * @return the tuple at the specified index
     */
    public Tuple getTuple(final int index, Tuple reuse);

    /**
     * Return the total number of tuples in the store
     * 
     * @return total number of tuples in the store
     */
    public int getNumTuples();

    /**
     * Get the schema of the data
     * 
     * @return the schema
     */
    public BlockSchema getSchema();

    /**
     * Returns an array of offsets that point to the data
     * 
     * @return array of offsets
     */
    public int[] getOffsets();

    /**
     * Sort the Tuple Store using Comparator Keys
     */
    public void sort(SortAlgo<Tuple> algo);

}
