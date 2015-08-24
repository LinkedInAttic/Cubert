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

package com.linkedin.cubert.memory;

import java.util.Arrays;
import java.util.List;

/**
 * A resizable list of integers that does not box/unbox the values to Integer objects.
 * <p>
 * This class supports a subset of methods from {@link java.util.List}:
 * <ul>
 * <li> {@link add} to add a value in the list</li>
 *
 * <li> {@link clear} to reset the list.</li>
 *
 * <li> {@link iterator} to obtain an {@link IntIterator} for values.</li>
 * </ul>
 * <p>
 * This class is not thread-safe. Also the iterator is not safe against concurrent
 * modifications (calling store() or clear() while iterating).
 * <p>
 * Implementation notes: the underlying storage is an ArrayList of int array (
 * {@code ArrayList<int[]>}). Each int array in the ArryaList is of fixed size (equal to
 * BatchSize).
 * 
 * @see IntSet
 * @author Maneesh Varshney
 * 
 */
public final class IntArrayList extends SegmentedArrayList
{
    private final List<int[]> list;

    public IntArrayList()
    {
        super();
        list = (List) super.list;
    }

    public IntArrayList(int batchSize)
    {
        super(batchSize);
        list = (List) super.list;
    }

    public void setDefaultValue(Integer defaultValue)
    {
        if (defaultValue == null)
            return;

        if (! (defaultValue instanceof  Integer))
            throw new RuntimeException("Non Integer used as default value for " + IntArrayList.class.getCanonicalName());

        super.setDefaultValue(defaultValue);
    }

    @Override
    public void add(Object value)
    {
        addInt(((Number) value).intValue());
    }

    @Override
    public Object get(int index)
    {
        return getInt(index);
    }

    @Override
    public int compareIndices(int i1, int i2)
    {
        final int l1 = getInt(i1);
        final int l2 = getInt(i2);
        return (l1 < l2 ? -1 : (l1 == l2 ? 0 : 1));
    }

    /**
     * Add an integer value to the list.
     *
     * @param value
     *            the value to add to list
     */
    public void addInt(int value)
    {
        ensureCapacity(size);

        int index = size % batchSize;
        int batch = size / batchSize;

        list.get(batch)[index] = value;

        size++;
    }

    @Override
    protected Object freshBatch(Object reuse)
    {
        int[] batch = (reuse != null) ? ((int[]) reuse) : new int[batchSize];

        if (defaultValue != null)
            Arrays.fill(batch, ((Integer) defaultValue).intValue());

        return batch;
    }

    public void updateInt(int location, int value)
    {
        int batch = location / batchSize;
        if (batch >= list.size())
            throw new RuntimeException("Specified update location is outside range. Use add API");

        int index = location % batchSize;
        list.get(batch)[index] = value;
    }

    public int getInt(int pointer)
    {
        int batch = pointer / batchSize;
        int index = pointer % batchSize;

        return list.get(batch)[index];
    }

    /**
     * Obtain an iterator for the list.
     *
     * @return an iterator for the list
     */
    public IntIterator iterator()
    {
        return new StoreIterator();
    }

    /**
     * Inner class that implements the iterator for this list.
     * 
     * @author Maneesh Varshney
     * 
     */
    private class StoreIterator implements IntIterator
    {
        private int pointer = 0;

        @Override
        public boolean hasNext()
        {
            return pointer < size;
        }

        @Override
        public int next()
        {
            int result = getInt(pointer);
            pointer++;

            return result;
        }

    }
}
