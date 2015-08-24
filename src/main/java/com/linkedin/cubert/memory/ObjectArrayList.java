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

import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang.NotImplementedException;


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
 * Implementation notes: the underlying storage is an ArrayList of T array (
 * {@code ArrayList<T[]>}). Each array in the ArrayList is of fixed size (equal to
 * BatchSize).
 *
 * @author Suvodeep Pyne
 *
 */
public final class ObjectArrayList extends SegmentedArrayList
{
    private final List<Object[]> list;
    private Comparator comparator = null;

    public ObjectArrayList()
    {
        super();
        list = (List) super.list;
    }

    public ObjectArrayList(int batchSize)
    {
        super(batchSize);
        list = (List) super.list;
    }

    /**
     * Add an integer value to the list.
     * 
     * @param value the value to add to list
     */
    public void add(Object value)
    {
        int batch = size / batchSize;
        while (batch >= list.size())
            list.add(new Object[batchSize]);

        int index = size % batchSize;
        list.get(batch)[index] = value;

        size++;
    }

    @Override
    public int compareIndices(int i1, int i2)
    {
        Comparable o1 = (Comparable) get(i1);
        Comparable o2 = (Comparable) get(i2);

        return o1.compareTo(o2);
    }

    @Override
    public void setComparator(Comparator comparator)
    {
        this.comparator = comparator;
    }

    /**
     * NOTE: Currently not implemented. Use IntArrayList as reference when this array is used in growable mode.
     * @param reuse
     * @return
     */
    @Override
    protected Object freshBatch(Object reuse)
    {
        throw new NotImplementedException();
    }

    public Object get(int pointer)
    {
        int batch = pointer / batchSize;
        int index = pointer % batchSize;

        return list.get(batch)[index];
    }
}
