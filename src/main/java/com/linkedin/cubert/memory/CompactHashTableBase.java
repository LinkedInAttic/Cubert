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

import java.nio.BufferOverflowException;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

import com.linkedin.cubert.operator.cube.DimensionKey;
import com.linkedin.cubert.utils.Pair;

/**
 * 
 * A compact hash table implementation with predictable memory consumption. This is
 * implemented to have precise control over the total amount of memory allocated. The key
 * idea is to stored a key and return an "index" where the value would be stored if an
 * array of values of size expectedHTSize were to be allocated.
 * 
 * Why is only index returned instead of storing the actual values? This way, we can avoid
 * the cost of storing different types of objects via a generic Object array and let the
 * derived classes define what type of values they are allocating. The derived classes
 * just need to allocate an array and used the returned index to index into the array.
 * 
 * @author Krishna Puttaswamy
 * 
 */

public class CompactHashTableBase
{
    private static int LOAD_FACTOR = 2;
    private final int NUMBER_OF_HASHTABLE_ENTRIES;
    private final int EXPECTED_VALUE_SIZE; // = 80 * 1024 * 1024; // serialized data array
    private final int DEFAULT_VALUE = -1;
    private int EXPECTED_SIZE = 0;

    private final int[] keyHashcodes; // hashcodes
    private final int[] offsetToData; // offset to data array
    private final int[] dimensionKeyArray; // where data is stored in int format

    private int dimensionKeyLength = 0;
    private int dataArrayOffset = 0; // offset to dimensionKey offset

    private int putCount = 0;

    private final Pair<Integer, Boolean> index = new Pair<Integer, Boolean>(-1, false);

    private final Pair<DimensionKey, Integer> entry =
            new Pair<DimensionKey, Integer>(null, null);

    public CompactHashTableBase(int dimensionKeyLength, int expectedHTSize)
    {
        this.dimensionKeyLength = dimensionKeyLength;

        EXPECTED_SIZE = expectedHTSize;
        NUMBER_OF_HASHTABLE_ENTRIES = LOAD_FACTOR * EXPECTED_SIZE;
        EXPECTED_VALUE_SIZE = dimensionKeyLength * EXPECTED_SIZE;

        keyHashcodes = new int[NUMBER_OF_HASHTABLE_ENTRIES];
        offsetToData = new int[NUMBER_OF_HASHTABLE_ENTRIES];
        dimensionKeyArray = new int[EXPECTED_VALUE_SIZE];

        // initialize everything to -1
        for (int i = 0; i < NUMBER_OF_HASHTABLE_ENTRIES; i++)
        {
            keyHashcodes[i] = DEFAULT_VALUE;
            offsetToData[i] = DEFAULT_VALUE;
        }
    }

    class KeyIndexIterator implements Iterator<Pair<DimensionKey, Integer>>
    {
        int count = 0;
        int offset = 0;

        public KeyIndexIterator(int count)
        {
            this.count = count;
        }

        @Override
        public boolean hasNext()
        {
            return count > 0;
        }

        @Override
        public Pair<DimensionKey, Integer> next()
        {
            count--;

            Pair<DimensionKey, Integer> obj = deserializedAndGetEntry(offset);
            offset += dimensionKeyLength;

            return obj;
        }

        @Override
        public void remove()
        {
            throw new NotImplementedException();
        }
    }

    public Iterator<Pair<DimensionKey, Integer>> getIterator()
    {
        return new KeyIndexIterator(putCount);
    }

    public int size()
    {
        return putCount;
    }

    public void clear()
    {
        dataArrayOffset = 0;
        putCount = 0;

        for (int i = 0; i < NUMBER_OF_HASHTABLE_ENTRIES; i++)
        {
            keyHashcodes[i] = DEFAULT_VALUE;
            offsetToData[i] = DEFAULT_VALUE;
        }
    }

    private int getValidHashCode(DimensionKey key)
    {
        int h = key.hashCode();
        if (h == Integer.MIN_VALUE)
            h = Integer.MAX_VALUE;
        return h < 0 ? -h : h;
    }

    // No need to have this pair any more
    public Pair<Integer, Boolean> lookupOrCreateIndex(DimensionKey key)
    {
        if (putCount >= EXPECTED_SIZE)
            throw new BufferOverflowException(); // "The hash table size has exceeded the expected size");

        int realIndex = -1;

        int hashcode = getValidHashCode(key);
        int offset = hashcode % NUMBER_OF_HASHTABLE_ENTRIES;

        while (keyHashcodes[offset] != DEFAULT_VALUE)
        {
            if (keyHashcodes[offset] == hashcode
                    && deserializedAndCompare(offsetToData[offset], key))
                break;

            offset += 1;
            offset = offset % NUMBER_OF_HASHTABLE_ENTRIES;
        }

        boolean isNewKey = false;
        if (keyHashcodes[offset] != hashcode)
        {
            // this is new put
            putCount++; // count of the unique number of keys;
            isNewKey = true;
            keyHashcodes[offset] = hashcode;
            offsetToData[offset] = writeToStore(key);
        }

        realIndex = offsetToData[offset] / dimensionKeyLength;

        index.setFirst(realIndex);
        index.setSecond(isNewKey);

        return index;
    }

    private int writeToStore(DimensionKey key)
    {
        int oldOffset = dataArrayOffset;

        int[] dimArray = key.getArray();

        for (int i = 0; i < dimArray.length; i++)
            dimensionKeyArray[dataArrayOffset++] = dimArray[i];

        return oldOffset;
    }

    public Pair<DimensionKey, Integer> deserializedAndGetEntry(int offset)
    {
        entry.setSecond(offset / dimensionKeyLength);

        int len = dimensionKeyLength;
        int[] data = new int[len];

        for (int i = 0; i < len; i++)
            data[i] = dimensionKeyArray[offset++];
        DimensionKey key = new DimensionKey(data);

        entry.setFirst(key);

        return entry;
    }

    public boolean deserializedAndCompare(int offset, DimensionKey key)
    {
        int len = dimensionKeyLength;

        int[] tocompare = key.getArray();
        if (tocompare.length != len)
            return false;

        for (int i = 0; i < tocompare.length; i++)
            if (tocompare[i] != dimensionKeyArray[offset++])
                return false;

        return true;
    }

    public Object deserializedAndGetValue(int offset)
    {
        return offset / dimensionKeyLength;
    }

    // final class MyEntry<K, V> implements Map.Entry<K, V>
    // {
    // private K key;
    // private V value;
    //
    // public MyEntry()
    // {
    // }
    //
    // public MyEntry(K key, V value)
    // {
    // this.key = key;
    // this.value = value;
    // }
    //
    // @Override
    // public K getKey()
    // {
    // return key;
    // }
    //
    // @Override
    // public V getValue()
    // {
    // return value;
    // }
    //
    // public void setKey(K key)
    // {
    // this.key = key;
    // }
    //
    // @Override
    // public V setValue(V value)
    // {
    // V old = this.value;
    // this.value = value;
    // return old;
    // }
    // }
}
