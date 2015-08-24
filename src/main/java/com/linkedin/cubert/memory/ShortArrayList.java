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

import java.util.List;
import org.apache.commons.lang.NotImplementedException;


/** Resizable optimized storage for short integers.
 *
 * @author Mani Parkhe
 */
public final class ShortArrayList extends SegmentedArrayList
{
    private final List<short[]> list;

    public ShortArrayList()
    {
        super();
        list = (List) super.list;
    }

    public ShortArrayList(int batchSize)
    {
        super(batchSize);
        list = (List) super.list;
    }

    @Override
    public void add(Object value)
    {
        addShort(((Number) value).shortValue());
    }

    @Override
    public Object get(int index)
    {
        return getShort(index);
    }

    @Override
    public int compareIndices(int i1, int i2)
    {
        return Short.compare(getShort(i1), getShort(i2));
    }

    /**
     * NOTE: Currently not implemented. Use IntArrayList as reference when this array is used in growable mode.
     * @param reuse
     * @return
     */
    @Override
    protected Object freshBatch(Object reuse)
    {
        throw new NotImplementedException();
    }

    public void addShort(short value)
    {
        int batch = size / batchSize;
        while (batch >= list.size())
            list.add(new short[batchSize]);

        int index = size % batchSize;
        list.get(batch)[index] = value;

        size++;
    }

    public short getShort(int pointer)
    {
        int batch = pointer / batchSize;
        int index = pointer % batchSize;

        return list.get(batch)[index];
    }

}
