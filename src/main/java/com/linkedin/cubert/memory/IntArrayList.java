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

import java.util.ArrayList;
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
public final class IntArrayList
{
    private static final int BatchSize = 1024;
    private final List<int[]> list = new ArrayList<int[]>();
    private int size = 0;

    /**
     * Add an integer value to the list.
     * 
     * @param value
     *            the value to add to list
     */
    public void add(int value)
    {
        int batch = size / BatchSize;
        while (batch >= list.size())
            list.add(new int[BatchSize]);

        int index = size % BatchSize;
        list.get(batch)[index] = value;

        size++;
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
     * Clear the items in the list.
     */
    public void clear()
    {
        size = 0;
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
            int batch = pointer / BatchSize;
            int index = pointer % BatchSize;

            pointer++;

            return list.get(batch)[index];
        }

    }
}
