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

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.utils.TupleStore;
import com.linkedin.cubert.utils.print;

/**
 * The LookUpTable is a memory efficient data structure for performing look ups.
 * This is primarily useful while performing a HASH JOIN operation. The is designed to work in conjunction
 * with the SerializedTupleStore.
 *
 * Constraints:
 *      The data structure needs to be initialized with all the data before any queries can be performed on the
 *      structure.
 *
 * Data Structures:
 *      A HashCode array is maintained where the index of the array would represent the HashCode % (size of the array)
 *      and the value would be the position or index of a sorted offset array.
 *      The sorted offset array is an array of offsets which point to a tuple in the SerializedTupleStore.
 *      The array is sorted by hashcode, hashkey.
 *
 * Algorithm:
 *      The algorithm is a 2 step fetch.
 *      Step 1> Compute Hash Code of Key
 *      Step 2> idx = HashCodeArray[HashCode] gives the index in the offset array to start looking
 *      Step 3> Linearly probe from idx onwards in the offset array and find the tuple which matches the hash key
 *
 * @author spyne on Oct 13, 2014
 */
public class LookUpTable implements Map<Tuple, List<Tuple>>
{
    /* Used for obtaining the offset value */
    private static final int SIGNBIT = Integer.MIN_VALUE;
    private static final int MASK = Integer.MAX_VALUE;
    private static final int SIZE_HASH_CODE_ARR = 1 << 22;

    /* IndexSortable: Used for quick sort of offsets */
    private final IndexedSortable sortable;
    
    /* Tuple Store data */
    private final TupleStore store;

    /* Schema attributes */
    private final BlockSchema schema;
    private final int[] comparatorIndices; // Indices of Attributes that need to be Compared

    /* Hash Table data structures */
    private final int[] hashCodeArr;
    private final int[] offsetArr;

    public LookUpTable(TupleStore store,
                       String[] comparatorKeys) throws IOException
    {
        this.store = store;
        schema = store.getSchema();

        comparatorIndices = createComparatorIndices(comparatorKeys);

        /* Hash Table Data Structures */
        hashCodeArr = new int[SIZE_HASH_CODE_ARR];
        offsetArr = store.getOffsets();
        Arrays.fill(hashCodeArr, -1);

        if (store instanceof ColumnarTupleStore)
            sortable = new ColumnarIndexedSortable();
        else
            sortable = new CachedIndexedSortable();
        buildTable();
    }

    private int[] createComparatorIndices(String[] comparatorKeys)
    {
        int[] keyIndices = new int[comparatorKeys.length];
        for (int i = 0; i < keyIndices.length; i++)
            keyIndices[i] = schema.getIndex(comparatorKeys[i]);
        return keyIndices;
    }

    private void buildTable() throws IOException
    {
        QuickSort quickSort = new QuickSort();

        long start, end;

        /* Sort the offsets array */
        start = System.currentTimeMillis();
        if (offsetArr.length > 1)
        {
            quickSort.sort(sortable, 0, offsetArr.length);
        }
        end = System.currentTimeMillis();
        print.f("LookUpTable: Sorted %d entries in %d ms", offsetArr.length, (end - start));

        /* Fill in the HashCode array */
        start = System.currentTimeMillis();
        int prevHashCode = -1;
        Tuple prevTuple = newTuple();
        Tuple t = newTuple();

        for (int i = 0; i < offsetArr.length; ++i)
        {
            t = store.getTuple(offsetArr[i], t);
            int hashCode = tupleHashCode(t);
            if (prevHashCode != hashCode)
            {
                hashCodeArr[hashCode] = i;
                prevHashCode = hashCode;
            }

            if (i == 0 || !compareKeys(prevTuple, t))
            {
                offsetArr[i] = offsetArr[i] | SIGNBIT;
            }

            /* Object Reuse: Swap the tuples instead of creating new ones */
            Tuple temp = t;
            t = prevTuple;
            prevTuple = temp;
        }
        end = System.currentTimeMillis();
        print.f("LookUpTable: Created HashCode Array for %d entries in %d ms", offsetArr.length, (end - start));
    }

    private boolean compareKeys(Tuple t1, Tuple t2) throws ExecException
    {
        for (int idx : comparatorIndices)
        {
            if (!t1.get(idx).equals(t2.get(idx)))
            {
                return false;
            }
        }
        return true;
    }

    public Tuple newTuple()
    {
        return TupleFactory.getInstance().newTuple(schema.getNumColumns());
    }

    @Override
    public int size()
    {
        return offsetArr.length;
    }

    @Override
    public boolean isEmpty()
    {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key)
    {
        try
        {
            return hashCodeArr[keyHashCode((Tuple) key)] != -1;
        } catch (ExecException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean containsValue(Object value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Tuple> get(Object keyObject)
    {
        Tuple key = (Tuple) keyObject;

        try
        {
            /* Get the hashcode of the tuple considering only the comparator keys */
            final int hashCode = keyHashCode(key);

            /* This is the start index in the offset array */
            final int startOffsetIdx = hashCodeArr[hashCode];
            if (startOffsetIdx == -1) return null;

            /* The end index is the start index of the next element unless its the last element */
            int nextHashCode = hashCode + 1;
            while (nextHashCode < hashCodeArr.length && hashCodeArr[nextHashCode] == -1)
            {
                ++nextHashCode;
            }
            final int endOffsetIdx = nextHashCode < hashCodeArr.length ? hashCodeArr[nextHashCode] : offsetArr.length;

            List<Tuple> tuples = new ArrayList<Tuple>();
            boolean found = false;
            for (int i = startOffsetIdx; i < endOffsetIdx; ++i)
            {
                int offset = offsetArr[i];
                if (offset < 0)
                {
                    if (found) break;
                    offset = offset & MASK;
                    final Tuple t = store.getTuple(offset, null);
                    if (matchesKey(key, t))
                    {
                        found = true;
                        tuples.add(t);
                    }
                }
                else if (found)
                {
                    offset = offset & MASK;
                    tuples.add(store.getTuple(offset, null));
                }
            }
            if( !found)
                return null;
            return tuples;
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private boolean matchesKey(Tuple key, Tuple t) throws ExecException
    {
        for (int i = 0; i < comparatorIndices.length; ++i)
        {
            if (!key.get(i).equals(t.get(comparatorIndices[i])))
            {
                return false;
            }
        }
        return true;
    }

    private int tupleHashCode(Tuple tuple) throws ExecException
    {
        final int PRIME = 31;
        long hashCode = 17;
        for (int idx : comparatorIndices)
        {
            hashCode = hashCode * PRIME + tuple.get(idx).hashCode();
        }
        if (hashCode < 0) hashCode = -hashCode;
        return (int) (hashCode % hashCodeArr.length);
    }

    private int keyHashCode(Tuple key) throws ExecException
    {
        final int PRIME = 31;
        long hashCode = 17;
        for (int i = 0; i < key.size(); i++)
        {
            hashCode = hashCode * PRIME + key.get(i).hashCode();
        }
        if (hashCode < 0) hashCode = -hashCode;
        return (int) (hashCode % hashCodeArr.length);
    }

    @Override
    public List<Tuple> remove(Object key)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear()
    {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Set<Tuple> keySet()
    {
        final int nKeyColumns = comparatorIndices.length;
        final TupleFactory factory = TupleFactory.getInstance();
        final Tuple reuse = newTuple();

        final Set<Tuple> keys = new HashSet<Tuple>();
        try
        {
            for (int offset : offsetArr)
            {
                /* For every new key the sign bit is set. Thus, ignore all others */
                if (offset >= 0)
                {
                    continue;
                }

                /* Mask out the offset and fetch from store */
                offset = offset & MASK;
                store.getTuple(offset, reuse);

                /* Create a key tuple and add it to the set */
                final Tuple t = factory.newTuple(nKeyColumns);
                for (int c = 0; c < nKeyColumns; ++c)
                {
                    t.set(c, reuse.get(comparatorIndices[c]));
                }
                keys.add(t);
            }
        }
        catch (ExecException e)
        {
            throw new RuntimeException(e);
        }
        return keys;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Collection<List<Tuple>> values()
    {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Set<Entry<Tuple, List<Tuple>>> entrySet()
    {
        throw new UnsupportedOperationException("entrySet method not implemented");
    }

    @Override
    public List<Tuple> put(Tuple key, List<Tuple> value)
    {
        throw new UnsupportedOperationException("put() called on read-only map");
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public void putAll(Map<? extends Tuple, ? extends List<Tuple>> m)
    {
        throw new UnsupportedOperationException("putAll() called on read-only map");
    }

    public void printTuples()
    {
        final Tuple reuse = newTuple();
        for (int offset : offsetArr)
        {
            System.out.println(store.getTuple(offset, reuse));
        }
    }

    public IndexedSortable getSortable()
    {
        return sortable;
    }

    class ColumnarIndexedSortable implements IndexedSortable
    {
        private Tuple t1;
        private Tuple t2;

        @Override
        public int compare(int i, int j)
        {
            try
            {
                final int offset1 = offsetArr[i] & MASK;
                final int offset2 = offsetArr[j] & MASK;

                t1 = store.getTuple(offset1, t1);
                t2 = store.getTuple(offset2, t2);

                /* t1 - t2 => ascending */
                int result = tupleHashCode(t1) - tupleHashCode(t2);
                if (result != 0) return result;

                for (int idx : comparatorIndices)
                {
                    Comparable a = (Comparable) t1.get(idx);
                    Comparable b = (Comparable) t2.get(idx);

                    /* ascending */
                    result = a.compareTo(b);
                    if (result != 0) return result;
                }
                return 0;
            }
            catch (IOException e)
            {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        @Override
        public void swap(int i, int j)
        {
            int temp = offsetArr[i];
            offsetArr[i] = offsetArr[j];
            offsetArr[j] = temp;
        }
    }


    /**
    * Created by spyne on 10/15/14.
    */
    class CachedIndexedSortable implements IndexedSortable
    {
        /* Objects reused */
        private final int[] offsets = { -1, -1, -1 };
        private final Tuple[] tuples;

        public CachedIndexedSortable()
        {
            tuples = new Tuple[]
            {
                    newTuple(),
                    newTuple(),
                    newTuple()
            };
        }

        /**
         * Copied from SerializedStoreTupleComparator
         */
        private Tuple getCached(final int offset) throws IOException
        {
            if (offsets[0] == offset)
                return tuples[0];

            if (offsets[1] == offset)
            {
                int tmp = offsets[0];
                offsets[0] = offsets[1];
                offsets[1] = tmp;

                Tuple ttmp = tuples[0];
                tuples[0] = tuples[1];
                tuples[1] = ttmp;

                return tuples[0];
            }

            int tmp0 = offsets[0];
            int tmp1 = offsets[1];
            offsets[0] = offsets[2];
            offsets[1] = tmp0;
            offsets[2] = tmp1;

            Tuple ttmp0 = tuples[0];
            Tuple ttmp1 = tuples[1];
            tuples[0] = tuples[2];
            tuples[1] = ttmp0;
            tuples[2] = ttmp1;

            if (offsets[0] != offset)
            {
                tuples[0] = store.getTuple(offset, tuples[0]);
                offsets[0] = offset;
            }

            return tuples[0];
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compare(int i, int j)
        {
            try
            {
                final int offset1 = offsetArr[i] & MASK;
                final int offset2 = offsetArr[j] & MASK;

                Tuple t1 = getCached(offset1);
                Tuple t2 = getCached(offset2);

                /* t1 - t2 => ascending */
                int result = tupleHashCode(t1) - tupleHashCode(t2);
                if (result != 0) return result;

                for (int idx : comparatorIndices)
                {
                    Comparable a = (Comparable) t1.get(idx);
                    Comparable b = (Comparable) t2.get(idx);

                    /* ascending */
                    result = a.compareTo(b);
                    if (result != 0) return result;
                }
                return 0;
            }
            catch (IOException e)
            {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        @Override
        public void swap(int i, int j)
        {
            int temp = offsetArr[i];
            offsetArr[i] = offsetArr[j];
            offsetArr[j] = temp;
        }
    }
}
