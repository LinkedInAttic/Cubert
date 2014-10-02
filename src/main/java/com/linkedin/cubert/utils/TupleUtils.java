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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.DataType;

/**
 * Various Tuple utility methods.
 * 
 * @author Maneesh Varshney
 * 
 */
public class TupleUtils
{

    public static void setLong(Tuple tuple, long value, int index, DataType type) throws ExecException
    {

        switch (type)
        {
        case BYTE:
            tuple.set(index, (byte) value);
            break;
        case INT:
            tuple.set(index, (int) value);
            break;
        case LONG:
            tuple.set(index, value);
            break;
        default:
            throw new IllegalArgumentException("Type " + type
                    + " cannot be converted from long");
        }
    }

    public static void setDouble(Tuple tuple, double value, int index, DataType type) throws ExecException
    {
        switch (type)
        {
        case DOUBLE:
            tuple.set(index, value);
            break;
        case FLOAT:
            tuple.set(index, (float) value);
            break;
        default:
            throw new IllegalArgumentException("Type " + type
                    + " cannot be converted from double");
        }
    }

    public static Tuple extractTupleWithReuse(Tuple fromTuple,
                                              BlockSchema schema,
                                              Tuple toTuple,
                                              String[] keys) throws ExecException
    {
        for (int i = 0; i < keys.length; i++)
            toTuple.set(i, fromTuple.get(schema.getIndex(keys[i])));

        return toTuple;
    }

    public static Tuple extractTuple(Tuple tuple, BlockSchema schema, String[] keys) throws ExecException
    {
        Tuple extracted = TupleFactory.getInstance().newTuple(keys.length);
        return extractTupleWithReuse(tuple, schema, extracted, keys);
    }

    public static void copy(Tuple src, Tuple dest) throws ExecException
    {
        int idx = 0;
        for (Object val : src.getAll())
        {
            dest.set(idx++, val);
        }
    }

    public static void deepCopy(Tuple src, Tuple dest) throws ExecException
    {
        int idx = 0;
        for (Object val : src.getAll())
        {
            dest.set(idx++, getFieldDeepCopy(val));
        }
    }

    public static Tuple getDeepCopy(Tuple originTuple) throws ExecException
    {
        Tuple copiedTuple = TupleFactory.getInstance().newTuple(originTuple.size());
        deepCopy(originTuple, copiedTuple);
        return copiedTuple;
    }

    private static DataBag getBagDeepCopy(DataBag originBag) throws ExecException
    {

        ArrayList<Tuple> copiedTuples = new ArrayList<Tuple>((int) originBag.size());
        for (Tuple t : originBag)
            copiedTuples.add(getDeepCopy(t));

        DataBag copiedDataBag = BagFactory.getInstance().newDefaultBag(copiedTuples);
        return copiedDataBag;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static Map getMapDeepCopy(Map originMap) throws ExecException
    {
        Map copiedMap = new HashMap(originMap.size());
        for (Map.Entry entry : (Set<Map.Entry>) originMap.entrySet())
        {
            assert (entry.getKey().getClass() == String.class);
            copiedMap.put(entry.getKey(), getFieldDeepCopy(entry.getValue()));
        }
        return copiedMap;
    }

    private static DataByteArray getByteArrayDeepCopy(DataByteArray originByteArray)
    {
        return new DataByteArray(originByteArray.get().clone());
    }

    private static Object getFieldDeepCopy(Object val) throws ExecException
    {
        if (val instanceof Tuple)
            return getDeepCopy((Tuple) val);
        else if (val instanceof DataBag)
            return getBagDeepCopy((DataBag) val);
        else if (val instanceof DataByteArray)
            return getByteArrayDeepCopy((DataByteArray) val);
        else if (val instanceof Map)
            return getMapDeepCopy((Map) val);

        return val;
    }
}
