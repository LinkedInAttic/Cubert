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

import com.linkedin.cubert.operator.cube.DimensionKey;
import com.linkedin.cubert.utils.Pair;
import java.util.Iterator;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


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
    private static final Log LOG = LogFactory.getLog(CompactHashTableBase.class.getName());

    private static final int LOAD_FACTOR = 2;
    private static final int BATCH_PCT = 10;

    private static final Integer DEFAULT_VALUE = new Integer(-1);
    private static final int GROWTH_TRACKER_SIZE = 10;

    private final int expectedSize;

    private final int keyHashtableBatchSize;
    private final IntArrayList batchGrowthTracker;

    private final IntArrayList keyHashcodes; // hashcodes
    private final IntArrayList offsetToData; // offset to data array
    private final IntArrayList dimensionKeyArray; // where data is stored in int format
    private int dimensionKeyLength = 0;

    private int dataArrayOffset = 0; // offset to dimensionKey offset

    private int numberOfHashtableEntries;
    private int putCount = 0;

    private final Pair<Integer, Boolean> index = new Pair<Integer, Boolean>(-1, false);

    private final Pair<DimensionKey, Integer> entry =
            new Pair<DimensionKey, Integer>(null, null);

    private final OffsetsIterator offsetsIterator = new OffsetsIterator();

    // Iterator abstraction for offset polling.
    class OffsetsIterator
    {
        private boolean first;
        private int offset;
        private int batchPos;

        public void setSeedOffset(int offset)
        {
            // Always look for offset starting in the first growth bucket.
            batchPos = 0;
            this.offset = offset % batchGrowthTracker.getInt(batchPos);
            first = true;
        }

        public int getNext()
        {
            if (first)
            {
                // method called for the first time.
                first = false;
                return offset;
            }

            // Increment offset
            offset++;

            if (offset == batchGrowthTracker.getInt(batchPos))
            {
                // Offset has hit the max count for current growth bucket.

                boolean firstWrapAround = (batchPos == 0);
                batchPos++;

                if (firstWrapAround)
                {
                    // ONLY For the first wrap-around only allow looking at offset == 0.
                    // TODO: optimize such that if initial offset was zero this condition can be skipped.

                    offset = 0;
                    return offset;
                }

                if (batchPos >= batchGrowthTracker.size())
                {
                    // Gone past the last growth bucket. Error!!
                    throw new RuntimeException("Hashtable offset not found!");
                }
            }

            return offset;
        }
    }

    /**
     * Note: size the hash table appropriately to fit in memory. Too small a hash table can affect runtime performance
     * due to bucket conflicts.
     *
     * @param dimensionKeyLength
     * @param expectedHTSize
     */
    public CompactHashTableBase(int dimensionKeyLength, int expectedHTSize)
    {
        this.dimensionKeyLength = dimensionKeyLength;

        expectedSize = expectedHTSize;
        numberOfHashtableEntries = baselineHashtableSize();

        int valueArraySize = dimensionKeyLength * expectedSize;

        keyHashcodes = getGrowableArray(numberOfHashtableEntries, DEFAULT_VALUE);
        offsetToData = getGrowableArray(numberOfHashtableEntries, DEFAULT_VALUE);
        dimensionKeyArray = getGrowableArray(valueArraySize, null);

        keyHashtableBatchSize = batchSize(numberOfHashtableEntries);

        batchGrowthTracker = new IntArrayList(GROWTH_TRACKER_SIZE);
        batchGrowthTracker.add(numberOfHashtableEntries); // initial size
    }

    private int baselineHashtableSize()
    {
        return LOAD_FACTOR * expectedSize;
    }

    private int batchSize(int baselineSize)
    {
        return (baselineSize * BATCH_PCT) / 100;
    }

    private IntArrayList getGrowableArray(int baselineSize, Integer defaultValue)
    {
        // Calculate size of each batch
        int batch_size = batchSize(baselineSize);
        IntArrayList growableArray = new IntArrayList(batch_size);

        // [Optionally] Set default value of each element in the array
        if (defaultValue != null)
            growableArray.setDefaultValue(defaultValue.intValue());

        // Ensure that array can hold baselineSize # of elements
        growableArray.ensureCapacity(baselineSize);

        return  growableArray;
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

        numberOfHashtableEntries = baselineHashtableSize();
        keyHashcodes.reset(numberOfHashtableEntries);
        offsetToData.reset(numberOfHashtableEntries);

        batchGrowthTracker.reset(GROWTH_TRACKER_SIZE);
        batchGrowthTracker.add(numberOfHashtableEntries);
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
        if (putCount >= numberOfHashtableEntries)
        {
            LOG.info("Bump size. putCount = " + putCount +
                " currentSize = " + numberOfHashtableEntries +
                " increase by = " + keyHashtableBatchSize);

            numberOfHashtableEntries += keyHashtableBatchSize;

            batchGrowthTracker.add(numberOfHashtableEntries); // track growth

            keyHashcodes.ensureCapacity(numberOfHashtableEntries);
            offsetToData.ensureCapacity(numberOfHashtableEntries);
        }

        int hashcode = getValidHashCode(key);
        offsetsIterator.setSeedOffset(hashcode);

        boolean isNewKey;
        int offset;

        // Look through offset enumerator to find empty / matching slot for key
        while (true)
        {
            offset = offsetsIterator.getNext();

            // Test: if the slot is empty
            if (keyHashcodes.getInt(offset) == DEFAULT_VALUE)
            {
                // Key does not exist, else would have been found before empty slot
                isNewKey = true;
                break;
            }

            // Test: if key @ slot matches current key
            if ((keyHashcodes.getInt(offset) == hashcode)
                && deserializedAndCompare(offsetToData.getInt(offset), key))
            {
                // Key exists. Rewrite
                isNewKey = false;
                break;
            }
        }

        if (isNewKey)
        {
            // new put.
            putCount++; // count of the unique number of keys;
            keyHashcodes.updateInt(offset, hashcode);
            offsetToData.updateInt(offset, writeToStore(key));
        }

        int realIndex = offsetToData.getInt(offset) / dimensionKeyLength;

        index.setFirst(realIndex);
        index.setSecond(isNewKey);

        return index;
    }

    private int writeToStore(DimensionKey key)
    {
        int oldOffset = dataArrayOffset;

        int[] dimArray = key.getArray();

        dimensionKeyArray.ensureCapacity(dataArrayOffset + dimensionKeyLength);
        for (int i = 0; i < dimArray.length; i++)
            dimensionKeyArray.updateInt(dataArrayOffset++, dimArray[i]);

        return oldOffset;
    }

    public Pair<DimensionKey, Integer> deserializedAndGetEntry(int offset)
    {
        entry.setSecond(offset / dimensionKeyLength);

        int len = dimensionKeyLength;
        int[] data = new int[len];

        for (int i = 0; i < len; i++)
            data[i] = dimensionKeyArray.getInt(offset++);
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
            if (tocompare[i] != dimensionKeyArray.getInt(offset++))
                return false;

        return true;
    }

    public Object deserializedAndGetValue(int offset)
    {
        return offset / dimensionKeyLength;
    }
}
