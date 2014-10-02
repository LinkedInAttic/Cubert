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

import java.util.Comparator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * Compares two tuples on specified column names.
 * 
 * @author Maneesh Varshney
 * 
 */
public class TupleComparator implements Comparator<Tuple>
{
    // private DataType[] dataTypes;
    private final int[] firstTupleIndex;
    private final int[] secondTupleIndex;
    private final DataType[] comparisonType;

    public TupleComparator(BlockSchema schema, String[] columns)
    {
        this(schema, columns, schema, columns);
    }

    public TupleComparator(BlockSchema firstSchema,
                           String[] firstColumns,
                           BlockSchema secondSchema,
                           String[] secondColumns)
    {
        if (firstColumns.length != secondColumns.length)
            throw new IllegalArgumentException("Number of columns to compare is not equal "
                    + firstColumns.length + " != " + secondColumns.length);

        int numColumns = firstColumns.length;
        firstTupleIndex = new int[numColumns];
        secondTupleIndex = new int[numColumns];
        comparisonType = new DataType[numColumns];

        for (int i = 0; i < numColumns; i++)
        {
            firstTupleIndex[i] = firstSchema.getIndex(firstColumns[i]);
            secondTupleIndex[i] = secondSchema.getIndex(secondColumns[i]);

            DataType firstType = firstSchema.getType(firstTupleIndex[i]);
            DataType secondType = secondSchema.getType(secondTupleIndex[i]);

            DataType widerType = DataType.getWiderType(firstType, secondType);
            comparisonType[i] = (widerType == null) ? DataType.UNKNOWN : widerType;
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static int compareObjects(Object o1, Object o2)
    {
        if (o1 == null)
            return o2 == null ? 0 : -1;

        if (o2 == null)
            return 1;

        return ((Comparable) o1).compareTo(o2);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static int compareObjects(Object o1, Object o2, DataType type)
    {
        if (o1 == null)
            return o2 == null ? 0 : -1;

        if (o2 == null)
            return 1;

        switch (type)
        {
        case INT:
        {
            int val1 = ((Number) o1).intValue();
            int val2 = ((Number) o2).intValue();
            return (val1 < val2) ? -1 : (val1 == val2 ? 0 : 1);
        }
        case LONG:
        {
            long val1 = ((Number) o1).longValue();
            long val2 = ((Number) o2).longValue();
            return (val1 < val2) ? -1 : (val1 == val2 ? 0 : 1);
        }
        case FLOAT:
        {
            float val1 = ((Number) o1).floatValue();
            float val2 = ((Number) o2).floatValue();
            return Float.compare(val1, val2);
        }
        case DOUBLE:
        {
            double val1 = ((Number) o1).doubleValue();
            double val2 = ((Number) o2).doubleValue();
            return Double.compare(val1, val2);
        }
        default:
            return ((Comparable) o1).compareTo(o2);
        }
    }

    @Override
    public int compare(Tuple tuple1, Tuple tuple2)
    {
        if (tuple1 == null)
            return tuple2 == null ? 0 : -1;

        if (tuple2 == null)
            return 1;

        int numColumns = firstTupleIndex.length;

        for (int i = 0; i < numColumns; i++)
        {
            int col1 = firstTupleIndex[i];
            int col2 = secondTupleIndex[i];

            try
            {
                Object o1 = tuple1.get(col1);
                Object o2 = tuple2.get(col2);

                int cmp = compareObjects(o1, o2, comparisonType[i]);

                if (cmp != 0)
                    return cmp;
            }
            catch (ExecException e)
            {
                throw new RuntimeException(e);
            }
        }

        return 0;
    }
}
