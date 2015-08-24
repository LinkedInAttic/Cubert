package com.linkedin.cubert.memory;

import com.linkedin.cubert.operator.cube.DimensionKey;
import com.linkedin.cubert.utils.DataGenerator;
import com.linkedin.cubert.utils.Pair;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Mani Parkhe
 */
public class TestCompactHashtable
{
    @Test
    public void testCompactHashtable()
    {
        CompactHashTableBase ht = new CompactHashTableBase(1, 1000);

        // data structures
        HashMap<Integer, Integer> value2index = new HashMap<Integer, Integer>();
        DimensionKey dk = new DimensionKey(new int[] { 4 });
        Pair<Integer, Boolean> indexPut, indexGet;

        // manual testing
        indexPut = ht.lookupOrCreateIndex(dk);
        Assert.assertEquals(indexPut.getSecond().booleanValue(), true);
        value2index.put(4, indexPut.getFirst().intValue());

        indexGet = ht.lookupOrCreateIndex(new DimensionKey(new int[] { 4 }));
        Assert.assertEquals(indexGet.getSecond().booleanValue(), false);
        Assert.assertEquals(indexGet.getFirst().intValue(), value2index.get(4).intValue());

        // automated testing
        DataGenerator dgen = new DataGenerator();
        dgen.setMAX_INT(10000);
        final int size = 4000;
        final int[] ints = dgen.randomInts(size);

        ht.clear();
        value2index.clear();

        // add
        for (int i : ints)
        {
            boolean wasInHt = value2index.containsKey(i);

            dk.set(0, i);
            indexPut = ht.lookupOrCreateIndex(dk);

            Assert.assertEquals(wasInHt, ! indexPut.getSecond().booleanValue());

            if (! wasInHt)
            {
                value2index.put(i, indexPut.getFirst());
            }
        }

        // test
        for (int i : ints)
        {
            dk.set(0, i);
            indexGet = ht.lookupOrCreateIndex(dk);

            Assert.assertEquals(indexGet.getSecond().booleanValue(), false);
            Assert.assertEquals(indexGet.getFirst().intValue(), value2index.get(i).intValue());
        }
    }
}
