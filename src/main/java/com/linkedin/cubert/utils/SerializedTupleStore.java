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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

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
import com.linkedin.cubert.io.CompactWritablesDeserializer;
import com.linkedin.cubert.io.DefaultTupleDeserializer;
import com.linkedin.cubert.io.DefaultTupleSerializer;
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
    private static int INITIAL_BYTE_ARRAY_SIZE = 1024 * 10;
    private int[] keyIndices;
    private ByteArrayOutputStream byteArrayOut;
    private List<Integer> startOffsetList;
    private final int numberOfFieldsinTuple;
    private final boolean createOffsetList;
    private int numTuples = 0;

    private Serializer<Tuple> serializer;
    private Deserializer<Tuple> writablesDeserializer;
    private Deserializer<Tuple> deserializer;

    public SerializedTupleStore(BlockSchema schema) throws IOException
    {
        this(schema, null);
    }

    public SerializedTupleStore(BlockSchema schema, String[] comparatorKeys) throws IOException
    {
        this.numberOfFieldsinTuple = schema.getNumColumns();
        this.createOffsetList = (comparatorKeys != null);
        this.byteArrayOut = new ByteArrayOutputStream(INITIAL_BYTE_ARRAY_SIZE);

        if (PhaseContext.getConf().getBoolean(CubertStrings.USE_COMPACT_SERIALIZATION,
                                              false)
                && schema.isFlatSchema())
        {
            serializer = new CompactSerializer<Tuple>(schema);
            writablesDeserializer = new CompactWritablesDeserializer<Tuple>(schema);
            deserializer = new CompactDeserializer<Tuple>(schema);
        }
        else
        {
            serializer = new DefaultTupleSerializer();
            deserializer = new DefaultTupleDeserializer();
            writablesDeserializer = deserializer;
        }

        serializer.open(byteArrayOut);

        if (createOffsetList)
        {
            startOffsetList = new ArrayList<Integer>();
            keyIndices = new int[comparatorKeys.length];
            for (int i = 0; i < keyIndices.length; i++)
                keyIndices[i] = schema.getIndex(comparatorKeys[i]);
        }
    }

    public void addToStore(Tuple tuple) throws IOException
    {
        int startOffset = byteArrayOut.size();
        serializer.serialize(tuple);

        numTuples++;

        if (createOffsetList)
        {
            startOffsetList.add(startOffset);
        }
    }

    public void clear()
    {
        byteArrayOut = null;
        if (startOffsetList != null)
            startOffsetList.clear();

        long before = Runtime.getRuntime().freeMemory();
        System.gc();
        long after = Runtime.getRuntime().freeMemory();
        print.f("Memory. Before=%d After=%d. Diff=%d", before, after, after - before);
    }

    public Map<Tuple, List<Tuple>> getHashTable() throws IOException
    {
        return new SerializedTupleMap(getBytes(), startOffsetList);
    }

    @Override
    public Iterator<Tuple> iterator()
    {
        if (createOffsetList)
            return new SerializedTupleStoreOffsetIterator(getBytes(), startOffsetList);
        else
            return new SerializedTupleStoreIterator(getBytes());
    }

    private byte[] getBytes()
    {
        return byteArrayOut.toByteArray();
    }

    public void sort()
    {
        System.gc();

        final SerializedTupleStoreReader reader =
                new SerializedTupleStoreReader(getBytes(), true);

        long start = System.currentTimeMillis();
        SerializedStoreTupleComparator comp = new SerializedStoreTupleComparator(reader);
        // Collections.sort(startOffsetList, comp);
        Integer[] array = startOffsetList.toArray(new Integer[0]);

        Arrays.sort(array, comp);

        ListIterator<Integer> i = startOffsetList.listIterator();
        for (int j = 0; j < array.length; j++)
        {
            i.next();
            i.set(array[j]);
        }

        long end = System.currentTimeMillis();
        print.f("Sorted in %d ms", end - start);

        // System.gc();
    }

    public int getNumTuples()
    {
        return numTuples;
    }

    public int size()
    {
        return byteArrayOut.size();
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
            tuples[0] = TupleFactory.getInstance().newTuple(numberOfFieldsinTuple);
            tuples[1] = TupleFactory.getInstance().newTuple(numberOfFieldsinTuple);
            tuples[2] = TupleFactory.getInstance().newTuple(numberOfFieldsinTuple);
        }

        private final Tuple getCached(int offset) throws IOException
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
                for (int i = 0; i < keyIndices.length; i++)
                {
                    Comparable<Object> left =
                            (Comparable<Object>) tuple1.get(keyIndices[i]);
                    Comparable<Object> right =
                            (Comparable<Object>) tuple2.get(keyIndices[i]);

                    if (left == null && right != null)
                        return -1;
                    if (left != null && right == null)
                        return 1;

                    if (left == null && right == null)
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
        private final Tuple tuple = TupleFactory.getInstance()
                                                .newTuple(numberOfFieldsinTuple);
        private int remaining;

        public SerializedTupleStoreIterator(final byte[] dataInBytes)
        {
            remaining = numTuples;

            try
            {
                deserializer.open(new ByteArrayInputStream(dataInBytes));
            }
            catch (IOException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
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
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }

            remaining--;

            return tuple;
        }

        @Override
        public void remove()
        {

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

        public SerializedTupleStoreOffsetIterator(final byte[] dataInBytes,
                                                  final List<Integer> startOffsetList)
        {
            this.reader = new SerializedTupleStoreReader(dataInBytes, false);
            offsetList = startOffsetList;
            tuple = TupleFactory.getInstance().newTuple(numberOfFieldsinTuple);
        }

        @Override
        public boolean hasNext()
        {
            if (position < offsetList.size())
                return true;
            else
                return false;
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
        private final ByteArrayInputStream in;
        private Deserializer<Tuple> des;

        public SerializedTupleStoreReader(byte[] dataInBytes,
                                          boolean useWritablesDeserializer)
        {
            in = new ByteArrayInputStream(dataInBytes);
            try
            {
                if (useWritablesDeserializer)
                    des = writablesDeserializer;
                else
                    des = deserializer;
                des.open(in);
            }
            catch (IOException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        // creates new tuples
        public Tuple getTupleAtOffset(int offset) throws IOException
        {
            in.reset();
            in.skip(offset);
            Tuple tuple = TupleFactory.getInstance().newTuple(numberOfFieldsinTuple);
            des.deserialize(tuple);

            return tuple;
        }

        // reuses the input tuple
        public Tuple getTupleAtOffset(int offset, Tuple tuple) throws IOException
        {
            in.reset();
            in.skip(offset);
            des.deserialize(tuple);

            return tuple;
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

        public SerializedTupleMap(byte[] serializedData, List<Integer> offsetList) throws IOException
        {
            tupleMap = new HashMap<Integer, HashMap<Integer, ArrayList<Integer>>>();
            this.offsetList = offsetList;

            reader = new SerializedTupleStoreReader(serializedData, false);
            tuple = TupleFactory.getInstance().newTuple(numberOfFieldsinTuple);

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
            for (int i = 0; i < offsetList.size(); i++)
            {
                int offset = offsetList.get(i);
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
                // TODO Auto-generated catch block
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
            if (size() == 0)
                return true;
            else
                return false;
        }

        @Override
        public boolean containsKey(Object key)
        {
            if (getInnerOffsetList((Tuple) key) != null)
                return true;
            else
                return false;
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
                        returnList.add(reader.getTupleAtOffset(offset));
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
            for (int i = 0; i < allOffsets.size(); i++)
            {
                try
                {
                    // need to make a copy of the tuples for the keySet
                    keyTuples.add(getProjectedKeyTuple(reader.getTupleAtOffset(allOffsets.get(i)),
                                                       true));
                }
                catch (IOException e)
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
}
