package com.linkedin.cubert.memory;

import com.linkedin.cubert.block.BlockSchema;
import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.lang.NotImplementedException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


/** Resizable store for nested bags. Store is represented as a Columnar Tuple Store (columns are represented using
 * primitives).
 *
 * A bag could store multiple primitives (like a bag of INTs) or multiple tuples. A TLB is used to translate the
 * index from caller to corresponding indices of elements in the bag.
 *
 * @author Mani Parkhe
 */
public class BagArrayList extends SegmentedArrayList
{
    private final ColumnarTupleStore store;
    private final Tuple tuple;
    private final ReusableDataBag outputBag;

    // TLB: to map index of bag (i) to indices of bag contents range: (tlb(i), tlb(i+1))
    // i+1-th element is guaranteed to be present
    private final IntArrayList translationBuffer;

    public BagArrayList(BlockSchema schema, boolean encodeStrings)
    {
        store = new ColumnarTupleStore(schema, encodeStrings);

        int nColumns = schema.getNumColumns();
        outputBag = new ReusableDataBag(nColumns);
        tuple = TupleFactory.getInstance().newTuple(nColumns);

        translationBuffer = new IntArrayList();

        // This is for the initial position
        translationBuffer.addInt(0);
    }

    @Override
    public void add(Object value)
    {
        DataBag bag = (DataBag) value;

        Iterator<Tuple> iter = bag.iterator();
        int counter = translationBuffer.getInt(size);
        while (iter.hasNext())
        {
            Tuple t = iter.next();
            counter++;

            try
            {
                store.addToStore(t);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        // adding the i+1-th element
        translationBuffer.addInt(counter);

        size++;
    }

    @Override
    public Object get(int index)
    {
        int startIndex = translationBuffer.getInt(index);
        int endIndex = translationBuffer.getInt(index + 1);

        outputBag.reset();

        for (int i = startIndex; i < endIndex; i++)
        {
            Tuple t = store.getTuple(i, tuple);
            try
            {
                outputBag.addTuple(t);
            }
            catch (ExecException e)
            {
                throw new RuntimeException(e);
            }
        }

        return outputBag;
    }

    @Override
    public int compareIndices(int i1, int i2)
    {
        throw new UnsupportedOperationException();
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
}
