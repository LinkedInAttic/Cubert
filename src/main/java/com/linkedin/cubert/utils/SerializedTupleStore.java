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

package com.linkedin.cubert.utils;

import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.TupleComparator;
import com.linkedin.cubert.io.CompactDeserializer;
import com.linkedin.cubert.io.CompactSerializer;
import com.linkedin.cubert.io.DefaultTupleDeserializer;
import com.linkedin.cubert.io.DefaultTupleSerializer;
import com.linkedin.cubert.memory.LookUpTable;
import com.linkedin.cubert.memory.PagedByteArray;
import com.linkedin.cubert.memory.PagedByteArrayInputStream;
import com.linkedin.cubert.memory.PagedByteArrayOutputStream;
import com.linkedin.cubert.operator.PhaseContext;
import com.linkedin.cubert.plan.physical.CubertStrings;

/**
 * A memory-efficient store for keeping PigTuples in memory. The SerializedStore keeps the
 * tuples in a byte[] after serializing them. This store supports obtaining a
 * {@code Map<Tuple,
 * List<Tuple>>} on the store, and also obtaining an iterator over sorted tuples from the
 * store. Note, however, that the map and the sorted iterator internally constructed over
 * the offsets of the tuples in the store and do not deserialize the tuples until a
 * specific tuple is requested.
 *
 * @author Krishna Puttaswamy
 *
 */

public class SerializedTupleStore implements TupleStore
{
    private static final int CHUNK_SIZE = 1 << 21; // 2 MB

    /* Reader class for random access of tuples */
    private final SerializedTupleStoreReader reader;

    /* Schema of the data */
    private BlockSchema schema;

    /* Record for number of tuples */
    private int numTuples = 0;

    /* Members used for serialization/deserialization */
    private Serializer<Tuple> serializer;
    private Deserializer<Tuple> writablesDeserializer;
    private Deserializer<Tuple> deserializer;

    /* The data stream */
    private PagedByteArrayOutputStream pbaos;

    /* Members used when comparator keys are present */
    private final String[] comparatorKeys;
    private boolean createOffsetList;
    private List<Integer> startOffsetList;
    private int[] keyIndices;

    public SerializedTupleStore(BlockSchema schema) throws IOException
    {
        this(schema, null);
    }

    public SerializedTupleStore(BlockSchema schema,String[] comparatorKeys) throws IOException
    {
        this.schema = schema;
        this.comparatorKeys = comparatorKeys;
        this.createOffsetList = (comparatorKeys != null);
        this.pbaos = new PagedByteArrayOutputStream(CHUNK_SIZE);

        if (PhaseContext.getConf().getBoolean(CubertStrings.USE_COMPACT_SERIALIZATION, false) && schema.isFlatSchema())
        {
            serializer = new CompactSerializer<Tuple>(schema);
            writablesDeserializer = new CompactDeserializer<Tuple>(schema);
            deserializer = new CompactDeserializer<Tuple>(schema);
        }
        else
        {
            serializer = new DefaultTupleSerializer();
            deserializer = new DefaultTupleDeserializer();
            writablesDeserializer = deserializer;
        }

        serializer.open(pbaos);

        if (createOffsetList)
        {
            startOffsetList = new ArrayList<Integer>();
            keyIndices = new int[comparatorKeys.length];
            for (int i = 0; i < keyIndices.length; i++)
                keyIndices[i] = schema.getIndex(comparatorKeys[i]);
        }

        reader = new SerializedTupleStoreReader(pbaos.getPagedByteArray(), true);
    }

    public void addToStore(Tuple tuple) throws IOException
    {
        int startOffset = pbaos.size();
        serializer.serialize(tuple);

        numTuples++;

        if (createOffsetList)
        {
            startOffsetList.add(startOffset);
        }
    }

    public void clear()
    {
        pbaos.reset();

        if (startOffsetList != null)
            startOffsetList.clear();

        long before = Runtime.getRuntime().freeMemory();
        System.gc();
        long after = Runtime.getRuntime().freeMemory();
        print.f("Memory. Before=%d After=%d. Diff=%d", before, after, after - before);
    }

    public Map<Tuple, List<Tuple>> getHashTable() throws IOException
    {
        return new LookUpTable(this, comparatorKeys);
    }

    @Override
    public Iterator<Tuple> iterator()
    {
        if (createOffsetList)
            return new SerializedTupleStoreOffsetIterator(pbaos.getPagedByteArray(), startOffsetList);
        else
            return new SerializedTupleStoreIterator(pbaos.getPagedByteArray());
    }

    public void sort(SortAlgo sa)
    {
        long start = System.currentTimeMillis();
        SerializedStoreTupleComparator comp = new SerializedStoreTupleComparator(reader);
        sa.sort(startOffsetList, comp);
        long end = System.currentTimeMillis();
        if (end - start > 10000)
        {
            print.f("SerializedTupleStore: Sorted %d tuples in %d ms", getNumTuples(), end - start);
        }
    }

    public int getNumTuples()
    {
        return numTuples;
    }

    public int size()
    {
        return pbaos.size();
    }

    /***
     * Comparator implementation to compare the StoreKeys.
     *
     * @author Krishna Puttaswamy
     *
     */
    class SerializedStoreTupleComparator implements Comparator<Integer>
    {
        SerializedTupleStoreReader reader;
        private final int[] offsets = { -1, -1, -1 };
        private final Tuple[] tuples = new Tuple[3];

        // 123 213 312
        // 213 123 321
        // 312 132 231
        // 321 231 132
        // 132 312 213
        // 231 321 123
        public SerializedStoreTupleComparator(SerializedTupleStoreReader reader)
        {
            this.reader = reader;
            tuples[0] = newTuple();
            tuples[1] = newTuple();
            tuples[2] = newTuple();
        }

        private Tuple getCached(int offset) throws IOException
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
                tuples[0] = reader.getTupleAtOffset(offset, tuples[0]);
                offsets[0] = offset;
            }

            return tuples[0];

        }

        @SuppressWarnings("unchecked")
        @Override
        public int compare(Integer o1, Integer o2)
        {
            try
            {
                Tuple tuple1 = getCached(o1);
                Tuple tuple2 = getCached(o2);

                int cmp = 0;
                for (int keyIndex : keyIndices)
                {
                    Comparable<Object> left = (Comparable<Object>) tuple1.get(keyIndex);
                    Comparable<Object> right = (Comparable<Object>) tuple2.get(keyIndex);

                    if (left == null && right != null)
                        return -1;
                    if (left != null && right == null)
                        return 1;

                    if (left == null) /* right == null is always true */
                        cmp = 0;
                    else
                        cmp = left.compareTo(right);

                    if (cmp != 0)
                        return cmp;
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }

            return 0;
        }
    }

    class SerializedTupleStoreIterator implements Iterator<Tuple>
    {
        private final Tuple tuple = newTuple();
        private final PagedByteArrayInputStream in;

        private int remaining;

        public SerializedTupleStoreIterator(final PagedByteArray pagedByteArray)
        {
            remaining = numTuples;
            in = new PagedByteArrayInputStream(pagedByteArray);

            try
            {
                deserializer.open(in);
            }
            catch (IOException e)
            {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean hasNext()
        {
            return remaining > 0;
        }

        @Override
        public Tuple next()
        {
            try
            {
                // tuple.readFields(in);
                deserializer.deserialize(tuple);
            }
            catch (IOException e)
            {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

            remaining--;

            return tuple;
        }

        @Override
        public void remove()
        {
            throw new NotImplementedException();
        }

    }

    /***
     * Gives an iterator over the offsetList; Note that the tuple that's returned by the
     * next() method is reused.
     *
     * @author Krishna Puttaswamy
     *
     */
    class SerializedTupleStoreOffsetIterator implements Iterator<Tuple>
    {
        private int position = 0;
        private final List<Integer> offsetList;
        private final SerializedTupleStoreReader reader;
        private final Tuple tuple;

        public SerializedTupleStoreOffsetIterator(PagedByteArray dataInBytes,
                                                  final List<Integer> startOffsetList)
        {
            this.reader = new SerializedTupleStoreReader(dataInBytes, false);
            offsetList = startOffsetList;
            tuple = newTuple();
        }

        @Override
        public boolean hasNext()
        {
            return position < offsetList.size();
        }

        @Override
        public Tuple next()
        {
            try
            {
                return reader.getTupleAtOffset(offsetList.get(position++), tuple);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public void remove()
        {
            throw new NotImplementedException();
        }
    }

    /**
     * A class to read the tuples based on offsets from the serialized byte[].
     *
     * @author Krishna Puttaswamy
     *
     */
    class SerializedTupleStoreReader
    {
        private final PagedByteArrayInputStream is;
        private final Deserializer<Tuple> deserializer;

        public SerializedTupleStoreReader(PagedByteArray pagedByteArray, boolean useWritablesDeserializer)
        {
            is = new PagedByteArrayInputStream(pagedByteArray);
            deserializer = useWritablesDeserializer ? writablesDeserializer : SerializedTupleStore.this.deserializer;
        }

        // reuses the input tuple
        public Tuple getTupleAtOffset(final int offset, Tuple reuse) throws IOException
        {
            if (reuse == null)
            {
                reuse = newTuple();
            }
            deserializer.open(is);
            is.reset();
            long skipped = is.skip(offset);
            if (skipped != offset)
            {
                throw new IOException("Unable to skip to offset: " + offset);
            }
            deserializer.deserialize(reuse);

            return reuse;
        }
    }

    @Override
    public Tuple getTuple(int index, Tuple reuse)
    {
        try
        {
            return reader.getTupleAtOffset(index, reuse);
        } catch (IOException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * Extracts the key part of the Tuple.
     *
     * @author Krishna Puttaswamy
     *
     */
    class SerializedStoreKey
    {
        private Object[] keys;
        private final int[] keyIndices;

        public SerializedStoreKey(int[] keyIndices) throws ExecException
        {
            this.keyIndices = keyIndices;
        }

        public void setKeyTuple(Tuple tuple) throws ExecException
        {
            keys = new Object[keyIndices.length];
            for (int i = 0; i < keyIndices.length; i++)
            {
                keys[i] = tuple.get(keyIndices[i]);
            }
        }

        public void set(Tuple tuple) throws ExecException
        {
            keys = new Object[keyIndices.length];
            for (int i = 0; i < keyIndices.length; i++)
            {
                keys[i] = tuple.get(i);
            }
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(keys);
            return result;
        }

        public int compare(Object[] array1, Object[] array2)
        {
            int numColumns = array1.length;

            for (int i = 0; i < numColumns; i++)
            {
                Object o1 = null;
                Object o2 = null;

                o1 = array1[i];
                o2 = array2[i];

                if (o1 == null && o2 == null)
                    continue;

                int cmp = TupleComparator.compareObjects(o1, o2);

                if (cmp != 0)
                    return cmp;
            }

            return 0;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            SerializedStoreKey other = (SerializedStoreKey) obj;
            if (compare(keys, other.keys) != 0)
                return false;
            return true;
        }

        @Override
        public String toString()
        {
            return String.format("SerializedStoreKey [keys=%s and hashCode=%d]",
                                 Arrays.toString(keys),
                                 this.hashCode());
        }
    }

    /**
     * Creates a Map of a Tuple to a List of Tuples with the same key. Internally the map
     * is maintained on the offsets to the tuples serialized and stored in a byte[].
     *
     * @author Krishna Puttaswamy
     *
     */
    class SerializedTupleMap implements Map<Tuple, List<Tuple>>
    {
        private final List<Integer> offsetList;
        private final Tuple tuple;
        private final SerializedTupleStoreReader reader;
        private final SerializedStoreKey oneKey, anotherKey;
        private final Tuple projectedKeyTuple;

        // This is how data is stored internally: The hashCode of the Key of an input
        // Tuple is the key to tupleMap;
        // The value is a hashMap where the key is the offset to the Tuple whose key part
        // is the key for the map;
        // The value of the internal hashMap is a List of offsets to the Tuples that have
        // the same key.
        HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> tupleMap;

        public SerializedTupleMap(PagedByteArray serializedData, List<Integer> offsetList) throws IOException
        {
            tupleMap = new HashMap<Integer, HashMap<Integer, ArrayList<Integer>>>();
            this.offsetList = offsetList;

            reader = new SerializedTupleStoreReader(serializedData, false);
            tuple = newTuple();

            oneKey = new SerializedStoreKey(keyIndices);
            anotherKey = new SerializedStoreKey(keyIndices);

            projectedKeyTuple = TupleFactory.getInstance().newTuple(keyIndices.length);

            createHashTable();
        }

        Tuple getProjectedKeyTuple(Tuple inputTuple, Boolean createNewTuple) throws ExecException
        {
            Tuple tempTuple;
            if (createNewTuple)
                tempTuple = TupleFactory.getInstance().newTuple(keyIndices.length);
            else
                tempTuple = projectedKeyTuple;

            for (int i = 0; i < keyIndices.length; i++)
                tempTuple.set(i, inputTuple.get(keyIndices[i]));

            return tempTuple;
        }

        private void createHashTable() throws IOException
        {
            // go over the tuples and build the hashMap on the offsets
            for (Integer offset : offsetList)
            {
                reader.getTupleAtOffset(offset, tuple);
                Tuple keyTuple = getProjectedKeyTuple(tuple, false);
                this.putTupleAndOffset(keyTuple, offset);
            }
        }

        private List<Integer> getInnerOffsetList(Tuple mytuple)
        {
            try
            {
                oneKey.set(mytuple);
                int key = oneKey.hashCode();

                // if the hash code matches, then we zoom in on to the internal hash table
                // of StoreKey to list of Tuples
                if (tupleMap.containsKey(key))
                {
                    HashMap<Integer, ArrayList<Integer>> subHashTable = tupleMap.get(key);
                    // go over the keyset of the internal hash table in the value field,
                    // and for each key of the HT retrieve and compare the tuple from the
                    // store at
                    // that offset

                    for (Integer tupleOffset : subHashTable.keySet())
                    {
                        anotherKey.setKeyTuple(reader.getTupleAtOffset(tupleOffset, tuple));
                        if (oneKey.equals(anotherKey))
                        {
                            return subHashTable.get(tupleOffset);
                        }
                    }
                }
            }
            catch (ExecException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }

            return null;
        }

        // the tuple should be the projected tuple
        private void putTupleAndOffset(Tuple mytuple, Integer offset)
        {
            Integer startOffsetOfTuple = offset;

            try
            {
                List<Integer> innerList = getInnerOffsetList(mytuple);
                if (innerList == null)
                {
                    oneKey.set(mytuple);
                    int key = oneKey.hashCode();

                    if (tupleMap.get(key) == null)
                        tupleMap.put(key, new HashMap<Integer, ArrayList<Integer>>());

                    HashMap<Integer, ArrayList<Integer>> subHashTable = tupleMap.get(key);
                    ArrayList<Integer> offsetList = new ArrayList<Integer>();
                    offsetList.add(startOffsetOfTuple);
                    subHashTable.put(startOffsetOfTuple, offsetList);
                }
                else
                {
                    innerList.add(offset);
                }

            }
            catch (ExecException e)
            {
                e.printStackTrace();
            }
        }

        @Override
        public int size()
        {
            return tupleMap.size();
        }

        @Override
        public boolean isEmpty()
        {
            return size() == 0;
        }

        @Override
        public boolean containsKey(Object key)
        {
            return getInnerOffsetList((Tuple) key) != null;
        }

        @Override
        public boolean containsValue(Object value)
        {
            throw new NotImplementedException();
        }

        @Override
        // get should be called only with the projected tuple that has only the key
        // portion of the tuple
        public List<Tuple> get(Object key)
        {
            Tuple mytuple = (Tuple) key;

            try
            {
                List<Integer> innerList = getInnerOffsetList(mytuple);

                if (innerList != null)
                {
                    List<Tuple> returnList = new ArrayList<Tuple>();
                    for (Integer offset : innerList)
                        returnList.add(getTuple(offset, null));
                    return returnList;
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

            return null;
        }

        @Override
        public List<Tuple> remove(Object key)
        {
            throw new NotImplementedException();
        }

        @Override
        public void clear()
        {
            tupleMap.clear();
        }

        @Override
        public Set<Tuple> keySet()
        {
            List<Integer> allOffsets = new ArrayList<Integer>();
            for (Integer hashcode : tupleMap.keySet())
            {
                allOffsets.addAll(tupleMap.get(hashcode).keySet());
            }

            Set<Tuple> keyTuples = new HashSet<Tuple>();
            for (Integer offset : allOffsets)
            {
                try
                {
                    // need to make a copy of the tuples for the keySet
                    keyTuples.add(getProjectedKeyTuple(getTuple(offset, null), true));
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
            }

            return keyTuples;
        }

        @Override
        public Collection<List<Tuple>> values()
        {
            throw new NotImplementedException();
        }

        @Override
        public Set<Map.Entry<Tuple, List<Tuple>>> entrySet()
        {
            throw new NotImplementedException();
        }

        @Override
        public List<Tuple> put(Tuple key, List<Tuple> value)
        {
            throw new UnsupportedOperationException("This is a read-only map. Put should not be called on this map.");
        }

        @Override
        public void putAll(Map<? extends Tuple, ? extends List<Tuple>> m)
        {
            throw new NotImplementedException();
        }
    }

    @Override
    public BlockSchema getSchema()
    {
        return schema;
    }

    @Override
    public int[] getOffsets()
    {
        int[] ret = new int[startOffsetList.size()];
        for (int i=0; i < ret.length; i++)
        {
            ret[i] = startOffsetList.get(i);
        }
        return ret;
    }

    public List<Integer> getStartOffsetList()
    {
        return startOffsetList;
    }

    public void dropIndex()
    {
        createOffsetList = false;
        startOffsetList = null;
    }

    public Tuple newTuple()
    {
        return TupleFactory.getInstance().newTuple(schema.getNumColumns());
    }
}
