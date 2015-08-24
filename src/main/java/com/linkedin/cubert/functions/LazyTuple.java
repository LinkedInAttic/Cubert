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

package com.linkedin.cubert.functions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * A Tuple that evaluates the fields on-demand.
 * <p>
 * For each field of the tuple, this object stores references to the functions (and their
 * input) that would generate this field. This tuple evaluates the function on-demand
 * (when the {@code get} method is called).
 * <p>
 * Note: only the {@code size} and {@code get} methods are implemented. All other methods
 * will throw {@code UnsupportedOperationException}
 * 
 * @author Maneesh Varshney
 * 
 */
public class LazyTuple implements Tuple
{
    private static final long serialVersionUID = 1203793026876410002L;
    private final FunctionTreeNode[] children;
    private List<Object> fields;

    public LazyTuple(FunctionTreeNode child)
    {
        children = new FunctionTreeNode[1];
        children[0] = child;
    }

    public LazyTuple(FunctionTreeNode[] children)
    {
        this.children = children;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(DataOutput arg0) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(Object o)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Object> iterator()
    {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    @Override
    public void reference(Tuple t)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size()
    {
        return children.length;
    }

    @Override
    public boolean isNull(int fieldNum) throws ExecException
    {
        return get(fieldNum) == null;
    }

    @Override
    public byte getType(int fieldNum) throws ExecException
    {
      return children[fieldNum].getType().getType().getPigDataType();
    }

    @Override
    public Object get(int fieldNum) throws ExecException
    {
        try
        {
            return children[fieldNum].eval();
        }
        catch (IOException e)
        {
            throw new ExecException(e);
        }
    }

    @Override
    public List<Object> getAll()
    {
        if (fields == null)
        {
            fields = new ArrayList<Object>(children.length);
            for (int i = 0; i < children.length; i++)
            {
                fields.add(null);
            }
        }
        for (int i = 0; i < children.length; i++)
        {
            try
            {
                fields.set(i, get(i));
            }
            catch (ExecException e)
            {
                throw new RuntimeException(e);
            }
        }
        return fields;
    }

    @Override
    public void set(int fieldNum, Object val) throws ExecException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void append(Object val)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMemorySize()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toDelimitedString(String delim) throws ExecException
    {
        throw new UnsupportedOperationException();
    }

}
