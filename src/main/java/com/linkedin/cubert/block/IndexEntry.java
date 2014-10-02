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

package com.linkedin.cubert.block;

import java.io.Serializable;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * Represents a single block for a relation stored in RUBIX format.
 * 
 * @author Maneesh Varshney
 * 
 */
public class IndexEntry implements Serializable, Comparable<IndexEntry>
{
    private static final long serialVersionUID = -1325514689713639584L;
    private final String file;
    private final long blockId;
    private Tuple key;
    private final long numRecords;
    private final long offset;
    private long length;
    private transient TupleComparator tupleComparator;

    public IndexEntry(String file,
                      Tuple key,
                      long offset,
                      long length,
                      long blockId,
                      long numRecords)
    {
        this.file = file;
        this.blockId = blockId;
        this.key = key;
        this.offset = offset;
        this.length = length;
        this.numRecords = numRecords;
    }

    public String getFile()
    {
        return file;
    }

    public Tuple getKey()
    {
        return key;
    }

    public long getBlockId()
    {
        return blockId;
    }

    public long getOffset()
    {
        return offset;
    }

    public long getLength()
    {
        return length;
    }

    public long getNumRecords()
    {
        return numRecords;
    }

    public void setLength(long length)
    {
        this.length = length;
    }

    void setKey(Tuple key)
    {
        this.key = key;
    }

    @Override
    public String toString()
    {
        return String.format("Split [key=%s, offset=%d, length=%d, blockid=%d, ReducerId=%d, file=%s]",
                             key,
                             offset,
                             length,
                             blockId,
                             getReducerId(),
                             file);
    }

    @Override
    public int compareTo(IndexEntry other)
    {
        try
        {
            return getTupleComparator().compare(key, other.key);
        }
        catch (ExecException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        final int reducerId = getReducerId();
        final int blockCount = (int) blockId;

        int result = 1;

        result = prime * result + reducerId;
        result = prime * result + blockCount;
        result = prime * result + ((file == null) ? 0 : file.hashCode());
        return result;
    }

    private int getReducerId()
    {
        return (int) (blockId >> 32);
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
        IndexEntry other = (IndexEntry) obj;
        if (blockId != other.blockId)
            return false;
        if (file == null)
        {
            if (other.file != null)
                return false;
        }
        else if (!file.equals(other.file))
            return false;
        return true;
    }

    private TupleComparator getTupleComparator() throws ExecException
    {
        if (tupleComparator == null)
        {
            // infer the schema of the key
            int numCols = key.size();
            String[] dummyColNames = new String[numCols];
            ColumnType[] colTypes = new ColumnType[numCols];
            for (int i = 0; i < numCols; i++)
            {
                dummyColNames[i] = "dummy_" + i;
                colTypes[i] =
                        new ColumnType(dummyColNames[i], DataType.getDataType(key.get(i)));
            }

            tupleComparator =
                    new TupleComparator(new BlockSchema(colTypes), dummyColNames);
        }
        return tupleComparator;
    }
}
