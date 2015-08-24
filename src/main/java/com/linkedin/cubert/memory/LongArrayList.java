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

import java.util.Arrays;
import java.util.List;

/**
 * A resizable list of longs that does not box/unbox the values to Long objects.
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
 * Implementation notes: the underlying storage is an ArrayList of long array (
 * {@code ArrayList<long[]>}). Each array in the ArrayList is of fixed size (equal to
 * batchSize).
 *
 * @author Suvodeep Pyne
 *
 */
public final class LongArrayList extends SegmentedArrayList
{
    private final List<long[]> list;

    public LongArrayList()
    {
        super();
        list = (List) super.list;
    }

    public LongArrayList(int batchSize)
    {
        super(batchSize);
        list = (List) super.list;
    }

    @Override
    public void add(Object value)
    {
        addLong(((Number) value).longValue());
    }

    @Override
    public Object get(int index)
    {
        return getLong(index);
    }

    @Override
    public int compareIndices(int i1, int i2)
    {
        final long l1 = getLong(i1);
        final long l2 = getLong(i2);
        return (l1 < l2 ? -1 : (l1 == l2 ? 0 : 1));
    }

    public void addLong(long value)
    {
        int batch = size / batchSize;
        while (batch >= list.size())
            list.add(new long[batchSize]);

        int index = size % batchSize;
        list.get(batch)[index] = value;

        size++;
    }

    @Override
    protected Object freshBatch(Object reuse)
    {
        long[] batch = (reuse != null) ? (long[]) (reuse) : new long[batchSize];

        if (defaultValue != null)
            Arrays.fill(batch, ((Long) defaultValue).longValue());

        return batch;
    }

    public void updateLong(int location, long value)
    {
        int batch = location / batchSize;
        if (batch >= list.size())
            throw new RuntimeException("Specified update location is outside range. Use add API");

        int index = location % batchSize;
        list.get(batch)[index] = value;
    }


    public long getLong(int pointer)
    {
        int batch = pointer / batchSize;
        int index = pointer % batchSize;

        return list.get(batch)[index];
    }
}
