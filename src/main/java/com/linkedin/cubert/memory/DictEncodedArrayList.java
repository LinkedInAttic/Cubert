package com.linkedin.cubert.memory;

import com.linkedin.cubert.utils.CodeDictionary;

/**
 * @author Maneesh Varshney
 */
public class DictEncodedArrayList extends SegmentedArrayList
{
    private final ShortArrayList codeList;
    private final CodeDictionary dictionary = new CodeDictionary();

    public DictEncodedArrayList(int batchSize)
    {
        codeList = new ShortArrayList(batchSize);
    }

    @Override
    public void add(Object value)
    {
        if (value == null)
            codeList.addShort((short) 0); // 0 is the code for null value

        // addKey() will first check if the key exists already
        int code = dictionary.addKey((String) value);
        codeList.addShort((short) code);

        size++;
    }

    @Override
    public Object get(int index)
    {
        int code = codeList.getShort(index);
        if (code == 0)
            return null;

        return dictionary.getValueForCode(code);
    }

    @Override
    public int compareIndices(int i1, int i2)
    {
        throw new UnsupportedOperationException();
    }
}
