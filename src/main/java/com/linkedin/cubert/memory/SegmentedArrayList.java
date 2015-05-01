/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.cubert.memory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * This class supports a subset of methods from {@link java.util.List}:
 * <ul>
 * <li> {@link add} to add a value in the list</li>
 *
 * <li> {@link clear} to reset the list.</li>
 *
 * </ul>
 * <p>
 * This class is not thread-safe. Also the iterator is not safe against concurrent
 * modifications (calling store() or clear() while iterating).
 * <p>
 * Implementation notes: the underlying storage is an ArrayList of object/primitive arrays.
 * Each array in the ArrayList is of fixed size (equal to BatchSize).
 *
 * Created by spyne on 1/9/15.
 */
public abstract class SegmentedArrayList
{
    protected final List<Object> list = new ArrayList<Object>();
    protected final int batchSize;
    protected int size = 0;

    public SegmentedArrayList()
    {
        batchSize = 1 << 10;
    }

    /**
     *
     * @param batchSize the size of each batch that is used for paging.
     */
    public SegmentedArrayList(int batchSize)
    {
        this.batchSize = batchSize;
    }

    /**
     * Add an Object to the list.
     *
     * @param value: the value to add to list
     */
    public abstract void add(Object value);

    /**
     * Returns the object at a specific index
     * @param index index
     * @return object at index
     */
    public abstract Object get(int index);

    /**
     * compare the objects at indices i1 and i2 and return compare(object(i1), object(i2))
     * @param i1 index 1
     * @param i2 index 2
     * @return compare(object(i1), object(i2))
     */
    public abstract int compareIndices(int i1, int i2);

    /**
     * Setter method for the comparator parameter
     *
     * @param comparator the comparator object
     */
    public void setComparator(Comparator comparator)
    {

    }

    /**
     * Clear the items in the list.
     */
    public void clear()
    {
        list.clear();
        size = 0;
    }

    /**
     *
     * @return the size of the list
     */
    public int size()
    {
        return size;
    }
}
