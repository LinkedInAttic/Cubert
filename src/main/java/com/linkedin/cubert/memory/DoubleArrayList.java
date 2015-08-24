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

import java.util.List;
import org.apache.commons.lang.NotImplementedException;


/**
 * A resizable list of doubles that does not box/unbox the values to Double objects.
 * <p>
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
 * Implementation notes: the underlying storage is an ArrayList of double array (
 * {@code ArrayList<double[]>}). Each array in the ArrayList is of fixed size (equal to
 * BatchSize).
 *
 * @author Suvodeep Pyne
 *
 */
public final class DoubleArrayList extends SegmentedArrayList
{
    private final List<double[]> list;

    public DoubleArrayList()
    {
        super();
        list = (List) super.list;
    }

    public DoubleArrayList(int batchSize)
    {
        super(batchSize);
        list = (List) super.list;
    }

    @Override
    public void add(Object value)
    {
        addDouble(((Number) value).doubleValue());
    }

    @Override
    public Object get(int index)
    {
        return getDouble(index);
    }

    @Override
    public int compareIndices(int i1, int i2)
    {
        return Double.compare(getDouble(i1), getDouble(i2));
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

    /**
     * Add an integer value to the list.
     * 
     * @param value the value to add to list
     */
    public void addDouble(double value)
    {
        int batch = size / batchSize;
        while (batch >= list.size())
            list.add(new double[batchSize]);

        int index = size % batchSize;
        list.get(batch)[index] = value;

        size++;
    }

    public double getDouble(int pointer)
    {
        int batch = pointer / batchSize;
        int index = pointer % batchSize;

        return list.get(batch)[index];
    }

}
