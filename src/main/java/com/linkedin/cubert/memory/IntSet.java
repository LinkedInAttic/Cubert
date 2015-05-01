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

import java.util.BitSet;

/**
 * A memory efficient set of positive integers that does not box values to Integer
 * objects. This class is optimized for integers that range from 0 to an upper bound
 * (typically index to an array).
 * <p>
 * This class supports a subset of methods in {@link java.util.Set}:
 * <ul>
 * <li>{@link add}: adds an integer to the set.</li>
 * 
 * <li>{@link iterator}: to obtain an iterator for the integers in the set</li>
 * 
 * <li>{@link clear}: clear the contents of the set.</li>
 * </ul>
 * <p>
 * This class is not thread safe. Also, the iterator is not safe against concurrent
 * modifications (calling add or clear when iterating).
 * <p>
 * Implementation notes: the underlying storage is {@link java.util.BitSet} and
 * {@link IntArrayList}. When adding an integer, the bit in the bitset at the index of
 * this integer is first checked. If not set, the bit is set to true and the value is
 * added to the array list; otherwise, nothing happens.
 * <p>
 * When clearing this set, the values in the list are iterated, and for each value, the
 * corresponding bit in the bitset is reset.
 * 
 * @see IntArrayList
 * @author Maneesh Varshney
 * 
 */
public class IntSet
{
    private final IntArrayList store = new IntArrayList();
    private final BitSet bitset = new BitSet();

    /**
     * Adds a value to the set.
     * 
     * @param value
     *            the value to add
     */
    public void add(int value)
    {
        if (!bitset.get(value))
        {
            bitset.set(value);
            store.addInt(value);
        }
    }

    /**
     * Returns an iterator for this set.
     * 
     * @return iterator of values in the set.
     */
    public IntIterator iterator()
    {
        return store.iterator();
    }

    /**
     * Clears the values in this set.
     */
    public void clear()
    {
        IntIterator it = store.iterator();
        while (it.hasNext())
            bitset.clear(it.next());

        store.clear();
    }
}
