package com.linkedin.cubert.memory;

import org.roaringbitmap.RoaringBitmap;

import java.util.BitSet;
import java.util.List;

/**
 * @author Maneesh Varshney
 */
public class FloatArrayList extends SegmentedArrayList
{
    private final List<float[]> list;
    private boolean hasNullValues;
    private RoaringBitmap isNullBitMap;

    public FloatArrayList()
    {
        super();
        list = (List) super.list;
    }

    public FloatArrayList(int batchSize)
    {
        super(batchSize);
        list = (List) super.list;
    }

    @Override
    public void add(Object value)
    {
        if (value == null)
        {
            if (!hasNullValues)
            {
                hasNullValues = true;
                isNullBitMap = new RoaringBitmap();
            }
            addFloat(0);
            isNullBitMap.add(size - 1);
            return;
        }
        addFloat(((Number) value).floatValue());
    }

    @Override
    public Object get(int index)
    {
        if (hasNullValues && isNullBitMap.contains(index))
            return null;

        return getFloat(index);
    }

    @Override
    public int compareIndices(int i1, int i2)
    {
        return Float.compare(getFloat(i1), getFloat(i2));
    }

    /**
     * Add an integer value to the list.
     *
     * @param value the value to add to list
     */
    public void addFloat(float value)
    {
        int batch = size / batchSize;
        while (batch >= list.size())
            list.add(new float[batchSize]);

        int index = size % batchSize;
        list.get(batch)[index] = value;

        size++;
    }

    public float getFloat(int pointer)
    {
        int batch = pointer / batchSize;
        int index = pointer % batchSize;

        return list.get(batch)[index];
    }
}
