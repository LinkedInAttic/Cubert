package com.linkedin.cubert.memory;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class ReusableDataBag implements DataBag
{
    /** */
    private static final long serialVersionUID = 1L;

    private final int numFields;
    private Tuple[] tuples = new Tuple[128];
    private int size = 0;

    public ReusableDataBag(int numFields)
    {
        this.numFields = numFields;
        for (int i = 0; i < 128; i++)
            tuples[i] = TupleFactory.getInstance().newTuple(numFields);
    }

    public void addTuple(Tuple tuple) throws ExecException
    {
        if (tuple.size() > numFields)
            throw new RuntimeException(String.format("Tuples in bag are expected to have %d fields. Presented : %d",
                                                     numFields,
                                                     tuple.size()));
        if (size >= tuples.length)
        {
            int origLength = tuples.length;
            tuples = Arrays.copyOf(tuples, tuples.length * 2);

            for (int i = origLength; i < tuples.length; i++)
                tuples[i] = TupleFactory.getInstance().newTuple(numFields);
        }

        Tuple t = tuples[size];
        for (int i = 0; i < tuple.size(); i++)
        {
            t.set(i, tuple.get(i));
        }

        size++;
    }

    public void add(Object... args) throws ExecException
    {
        if (args.length > numFields)
            throw new RuntimeException(String.format("Tuples in bag are expected to have %d fields. Presented : %d",
                                                     numFields,
                                                     args.length));
        if (size >= tuples.length)
        {
            int origLength = tuples.length;
            tuples = Arrays.copyOf(tuples, tuples.length * 2);

            for (int i = origLength; i < tuples.length; i++)
                tuples[i] = TupleFactory.getInstance().newTuple(numFields);
        }

        Tuple tuple = tuples[size];
        int pos = 0;
        for (Object obj : args)
            tuple.set(pos++, obj);

        size++;
    }

    public Tuple getTuple(int index)
    {
        return tuples[index];
    }

    public void reset()
    {
        size = 0;
    }

    @Override
    public long getMemorySize()
    {
        return 0;
    }

    @Override
    public long spill()
    {
        throw new UnsupportedOperationException();
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
    public void add(Tuple arg0)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addAll(DataBag arg0)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDistinct()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSorted()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Tuple> iterator()
    {
        return new ReusableDataBagIterator();
    }

    @Override
    public void markStale(boolean arg0)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long size()
    {
        return size;
    }

    public final class ReusableDataBagIterator implements Iterator<Tuple>
    {
        int counter = 0;

        @Override
        public boolean hasNext()
        {
            return counter < size;
        }

        @Override
        public Tuple next()
        {
            return tuples[counter++];
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("ReusableDataBag [tuples=")
               .append(Arrays.toString(tuples))
               .append("]");
        return builder.toString();
    }

}
