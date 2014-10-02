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

package com.linkedin.cubert.operator.cube;

import java.util.Arrays;

/**
 * Specifies the dimension columns. All the dimensions are stored as ints. We expect the
 * input dimension to be either a int or a long. In case a long is input, the long is
 * split into two ints and stored in the int array.
 * 
 * This object can enumerate the "ancestors" via the {@code ancestors} method.
 * 
 * @author Maneesh Varshney
 * 
 * @author Krishna Puttaswamy
 * 
 */
public class DimensionKey
{
    private final int[] dimensionsInInts;

    public DimensionKey(int dimSizeInInts)
    {
        if (dimSizeInInts > 32)
            throw new IllegalArgumentException("Only upto 32 dimensions are supported.");

        // one additional int is allocated to store the metadata about the
        // dimensions that are 0 indicating "all" in OLAP cube
        this.dimensionsInInts = new int[dimSizeInInts + 1];
    }

    public DimensionKey(int[] input)
    {
        this.dimensionsInInts = new int[input.length];
        System.arraycopy(input, 0, dimensionsInInts, 0, dimensionsInInts.length);
    }

    public DimensionKey(DimensionKey other)
    {
        dimensionsInInts = new int[other.dimensionsInInts.length];
        System.arraycopy(other.dimensionsInInts,
                         0,
                         dimensionsInInts,
                         0,
                         dimensionsInInts.length);
    }

    public void set(int index, int dimValue)
    {
        dimensionsInInts[index] = dimValue;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(dimensionsInInts);
        return result;
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
        DimensionKey other = (DimensionKey) obj;
        if (!Arrays.equals(dimensionsInInts, other.dimensionsInInts))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        if (dimensionsInInts == null)
            return "null";

        StringBuilder b = new StringBuilder();

        for (int i = 0; i < dimensionsInInts.length; i++)
        {
            b.append(dimensionsInInts[i]);
            if (i != dimensionsInInts.length - 1)
                b.append(",");
        }

        return b.toString();
    }

    public int[] getArray()
    {
        return dimensionsInInts;
    }
}
