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

package com.linkedin.cubert.io.rubix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 * @author spyne
 * 
 * @param <K>
 */
public class RubixFileKeyData<K> implements Writable
{
    /* transient variables */
    private long length = 0;

    /* persistent data */
    private final K key;
    private long blockId;
    private long offset;
    private long numRecords = 0;

    public RubixFileKeyData(K key, long blockId, long offset, long numRecs)
    {
        this.key = key;
        this.blockId = blockId;
        this.offset = offset;
        this.numRecords = numRecs;
    }

    public RubixFileKeyData(K key)
    {
        this.key = key;
    }

    public K getKey()
    {
        return key;
    }

    public long getBlockId()
    {
        return blockId;
    }

    public long getNumRecords()
    {
        return numRecords;
    }

    public long getOffset()
    {
        return offset;
    }

    public long getLength()
    {
        return length;
    }

    void setLength(long length)
    {
        this.length = length;
    }

    public void incrementNumRecords()
    {
        ++this.numRecords;
    }

    @Override
    public String toString()
    {
        return String.format("KeyData [key=%s, offset=%d, length=%d, numRecords=%d]",
                             key,
                             offset,
                             length,
                             numRecords);
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        if (!(key instanceof Writable))
            throw new RuntimeException("Key Class is not Writable");
        Writable keyWritable = (Writable) key;

        keyWritable.write(out);
        out.writeLong(blockId);
        out.writeLong(offset);
        out.writeLong(numRecords);

    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        ((Writable) key).readFields(in);
        blockId = in.readLong();
        offset = in.readLong();
        numRecords = in.readLong();
    }
}
