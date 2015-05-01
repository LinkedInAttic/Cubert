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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.linkedin.cubert.operator.PhaseContext;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.AbstractTuple;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.TupleStoreBlock;
import com.linkedin.cubert.utils.SortAlgo;
import com.linkedin.cubert.utils.TupleStore;

/**
 * Columnar Tuple Store
 *      ColumnarTupleStore is an implementation of columnar store of pig tuples in Cubert. Currently, this data
 *      structure only supports flat schema. For primitive data types the store uses a segmented implementation of
 *      primitive arrays which makes it more memory efficient.
 *
 * @author spyne
 * Created on 1/12/15.
 */
public class ColumnarTupleStore implements TupleStore
{
    private final BlockSchema schema;
    private final int nColumns;


    /* Number of rows in the store */
    private int size;

    /* Data Store */
    private final SegmentedArrayList[] dataStore;

    /* Start and end indices for subrange operation */
    private int start = 0;

    public ColumnarTupleStore(BlockSchema schema)
    {
        this(schema, false);
    }

    public ColumnarTupleStore(BlockSchema schema, boolean encodeStrings)
    {
        // TOOD: set up the valid schema test here
        this.schema = schema;
        nColumns = schema.getNumColumns();

        dataStore = new SegmentedArrayList[nColumns];
        prepareDataStore(encodeStrings);
    }

    public ColumnarTupleStore(ColumnarTupleStore store, int start, int size)
    {
        this.start = start;
        this.size = size;

        dataStore = store.dataStore;
        schema = store.getSchema();
        nColumns = schema.getNumColumns();
    }

    private void prepareDataStore(boolean encodeStrings)
    {
        final int batchSize = 1 << 10;
        ColumnType[] columnTypes = schema.getColumnTypes();


        for (int i = 0; i < columnTypes.length; i++)
        {
            ColumnType colType = columnTypes[i];
            switch (colType.getType())
            {
                case INT:
                    dataStore[i] = new IntArrayList(batchSize);
                    break;
                case LONG:
                    dataStore[i] = new LongArrayList(batchSize);
                    break;
                case DOUBLE:
                    dataStore[i] = new DoubleArrayList(batchSize);
                    break;
                case FLOAT:
                    dataStore[i] = new FloatArrayList(batchSize);
                    break;
                case STRING:
                    if (encodeStrings)
                        dataStore[i] = new DictEncodedArrayList(batchSize);
                    else
                        dataStore[i] = new ObjectArrayList(batchSize);
                    break;
                case BAG:
                    dataStore[i] = new BagArrayList(
                            colType.getColumnSchema().getColumnType(0).getColumnSchema(),
                            encodeStrings);
                    break;
                default:
                    dataStore[i] = new ObjectArrayList(batchSize);
                    break;
            }
        }
    }

    @Override
    public void addToStore(Tuple tuple) throws IOException
    {
        for (int i = 0; i < nColumns; ++i)
        {
            dataStore[i].add(tuple.get(i));
        }
        ++size;
    }

    @Override
    public void clear()
    {
        for (int i = 0; i < nColumns; ++i)
        {
            dataStore[i].clear();
        }
        size = 0;
    }

    @Override
    public Iterator<Tuple> iterator()
    {
        return new Iterator<Tuple>()
        {
            private int count = 0;
            private int index = start;
            private ColumnStoreTuple tuple = new ColumnStoreTuple(start);

            @Override
            public boolean hasNext()
            {
                return count < size;
            }

            @Override
            public Tuple next()
            {
                tuple.rowid = index;
                ++index;
                ++count;
                return tuple;
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException("Not available in a Tuple Store");
            }
        };
    }

    @Override
    public Tuple getTuple(int index, Tuple reuse)
    {
        if (reuse == null)
        {
            return new ColumnStoreTuple(index);
        }
        else if (reuse instanceof ColumnStoreTuple)
        {
            ((ColumnStoreTuple) reuse).rowid = index;
            return reuse;
        }
        try
        {
            for (int i = 0; i < nColumns; ++i)
            {
                reuse.set(i, dataStore[i].get(index));
            }
            return reuse;
        }
        catch (ExecException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public Tuple newTuple()
    {
        return TupleFactory.getInstance().newTuple(schema.getNumColumns());
    }

    @Override
    public int getNumTuples()
    {
        return size;
    }

    @Override
    public BlockSchema getSchema()
    {
        return schema;
    }

    @Override
    public int[] getOffsets()
    {
        final int[] indexes = new int[size];
        for (int i = 0; i < indexes.length; i++)
        {
            indexes[i] = i;
        }
        return indexes;
    }

    @Override
    public void sort(SortAlgo<Tuple> algo)
    {
        throw new UnsupportedOperationException("Not Implemented yet.");
    }

    public Map<Object, Block> getSubBlocksForPartitionCol(String colName) throws ExecException
    {
        final Map<Object, Block> subBlockMap = new HashMap<Object, Block>();

        final SegmentedArrayList list = dataStore[schema.getIndex(colName)];

        for (int start = 0, i = 0; i < size; ++i)
        {
            if (i+1 == size || list.compareIndices(i, i+1) != 0)
            {
                Block block = new TupleStoreBlock(
                        new ColumnarTupleStore(this, start, i - start + 1),
                        new BlockProperties("block" + start, schema, (BlockProperties[]) null));
                subBlockMap.put(list.get(i), block);

                start = i+1;
            }
        }
        return subBlockMap;
    }

    public class ColumnStoreTuple extends AbstractTuple implements PrimitiveTuple
    {
        private int rowid;

        public ColumnStoreTuple(int rowid)
        {
            this.rowid = rowid;
        }

        @Override
        public int size()
        {
            return nColumns;
        }

        @Override
        public Object get(int fieldNum) throws ExecException
        {
            return dataStore[fieldNum].get(rowid);
        }

        public long getLong(int fieldNum)
        {
            return ((LongArrayList) dataStore[fieldNum]).getLong(rowid);
        }

        public int getInt(int fieldNum)
        {
            return ((IntArrayList) dataStore[fieldNum]).getInt(rowid);
        }

        public double getDouble(int fieldNum)
        {
            return ((DoubleArrayList) dataStore[fieldNum]).getDouble(rowid);
        }

        @Override
        public List<Object> getAll()
        {
            List<Object> fields = new ArrayList<Object>(nColumns);
            try
            {
                for (int i = 0; i < nColumns; ++i)
                {
                    fields.add(get(i));
                }
                return fields;
            }
            catch (ExecException e)
            {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        @Override
        public void set(int fieldNum, Object val) throws ExecException
        {
            throw new UnsupportedOperationException("Not Implemented");
        }

        @Override
        public void append(Object val)
        {
            throw new UnsupportedOperationException("Not Implemented");
        }

        @Override
        public long getMemorySize()
        {
            throw new UnsupportedOperationException("Not Implemented");
        }

        /**
         * Implementation is copied from DefaultTuple implementation in pig.
         *
         * @param obj the other object to be compared
         * @return -1 if the other is less, 0 if equal or 1 if the other is greater to this object
         */
        @Override
        public int compareTo(Object obj)
        {
            if (obj instanceof Tuple)
            {
                Tuple other = (Tuple) obj;
                int otherSize = other.size();
                if (otherSize < nColumns) return 1;
                else if (otherSize > nColumns) return -1;
                else
                {
                    for (int i = 0; i < nColumns; i++)
                    {
                        try
                        {
                            int c = DataType.compare(get(i), other.get(i));
                            if (c != 0) return c;
                        }
                        catch (ExecException e)
                        {
                            throw new RuntimeException("Unable to compare tuples", e);
                        }
                    }
                    return 0;
                }
            }
            else
            {
                return DataType.compare(this, obj);
            }
        }

        @Override
        public void write(DataOutput out) throws IOException
        {
            throw new UnsupportedOperationException("Not Implemented");
        }

        @Override
        public void readFields(DataInput in) throws IOException
        {
            throw new UnsupportedOperationException("Not Implemented");
        }
    }
}
