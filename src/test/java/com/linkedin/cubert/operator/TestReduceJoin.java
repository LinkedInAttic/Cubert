package com.linkedin.cubert.operator;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.linkedin.cubert.utils.JsonUtils.createArrayNode;
import static com.linkedin.cubert.utils.JsonUtils.createObjectNode;

/**
 * @author Maneesh Varshney
 */
public class TestReduceJoin
{
//    @BeforeClass
    public void setUp() throws IOException
    {
    }

    private void test(Object[][] input, int numJoinKeys, boolean isOuter, Object[][] expected)
            throws PreconditionException, IOException, InterruptedException
    {
        int numColumns = input[0].length;
        String[] colNames = new String[numColumns];
        String[] joinKeys = new String[numJoinKeys];

        for (int i = 0; i < numColumns - 1; i++)
            colNames[i] = "col_" + i;
        colNames[numColumns - 1] = "___tag";

        for (int i = 0; i < numJoinKeys; i++)
            joinKeys[i] = "col_" + i;

        Block block =
                new ArrayBlock(Arrays.asList(input), colNames);

        TupleOperator operator = new RSJoinOperator();


        BlockSchema inSchema = block.getProperties().getSchema();
        PostCondition condition = new PostCondition(inSchema, null, null);
        JsonNode json = createObjectNode("joinKeys", createArrayNode(joinKeys));
        if (isOuter)
            ((ObjectNode) json).put("joinType", "LEFT OUTER");
        BlockSchema outSchema = operator.getPostCondition(makeMap("", condition), json).getSchema();

        BlockProperties props = new BlockProperties("", outSchema, (BlockProperties) null);

        operator.setInput(makeMap("", block), json, props);

        Tuple expectedTuple = TupleFactory.getInstance().newTuple(numColumns - 1);

        for (int idx = 0; idx < expected.length; idx++)
        {
            Tuple tuple = operator.next();
            Assert.assertNotNull(tuple);

            for (int i = 0; i < numColumns - 1; i++)
            {
                expectedTuple.set(i, expected[idx][i]);
            }
            Assert.assertEquals(tuple, expectedTuple);
        }

        Assert.assertNull(operator.next());
    }

    private void test(Object[][] input, int numJoinKeys, Object[][] expected)
            throws PreconditionException, IOException, InterruptedException
    {
        test(input, numJoinKeys, false, expected);
    }

    @Test
    public void testReduceInnerJoin() throws PreconditionException, IOException, InterruptedException
    {
        // simple case: one pivot only
        Object[][] rows = { { 1000, null, 100, 0 },
                            { 1000, 20, null, 2 },
                            { 1000, 21, null, 2 }};

        Object[][] expected = {{1000, 20, 100}, {1000, 21, 100}};

        test(rows, 1, expected);

        // one pivot; no right rows
        rows = new Object[][]{ { 1000, 20, null, 2 },
                               { 1000, 20, null, 2 },
                               { 1000, 21, null, 2 }};

        expected = new Object[][] {};

        test(rows, 1,expected);

        // one pivot; no left rows
        rows = new Object[][]{ { 1000, 20, null, 0 }};
        expected = new Object[][] {};
        test(rows, 1, expected);

        // two pivots; simple case
        rows = new Object[][]{ { 1000, null, 100, 0 },
                            { 1000, 20, null, 2 },
                            { 1001, null, 101, 0 },
                            { 1001, 21, null, 2 }};

        expected = new Object[][]{{1000, 20, 100}, {1001, 21, 101}};

        test(rows, 1, expected);

        // multi pivots; no right rows
        rows = new Object[][]{ { 1000, 20, null, 2 },
                               { 1001, 20, null, 2 },
                               { 1002, 21, null, 2 }};

        expected = new Object[][] {};

        test(rows, 1, expected);

        // multi pivots; no left rows
        rows = new Object[][]{ { 1000, 20, null, 0 },
                               { 1001, 20, null, 0 },
                               { 1002, 20, null, 0 }};
        expected = new Object[][] {};
        test(rows, 1, expected);

        // multipivots: hasleft - noleft - hasleft
        rows = new Object[][]{ { 1000, null, 20, 0 },
                               { 1000, 100, null, 2 },
                               { 1001, null, 20, 0 },
                               { 1002, null, 21, 0 },
                               { 1002, 101, null, 2 }};

        expected = new Object[][] {{1000, 100, 20}, {1002, 101, 21}};
        test(rows, 1, expected);

        // multipivots: hasleft - noright - hasleft
        rows = new Object[][]{ { 1000, null, 20, 0 },
                               { 1000, 100, null, 2 },
                               { 1001, null, 20, 2 },
                               { 1002, null, 21, 0 },
                               { 1002, 101, null, 2 }};

        expected = new Object[][] {{1000, 100, 20}, {1002, 101, 21}};
        test(rows, 1, expected);
    }

    @Test
    public void testReduceLeftOuterJoin() throws PreconditionException, IOException, InterruptedException
    {
        // simple case: one pivot only
        Object[][] rows = { { 1000, null, 100, 0 },
                            { 1000, 20, null, 2 },
                            { 1000, 21, null, 2 }};

        Object[][] expected = {{1000, 20, 100}, {1000, 21, 100}};

        test(rows, 1, true, expected);

        // one pivot; no right rows
        rows = new Object[][]{ { 1000, 20, null, 2 },
                               { 1000, 20, null, 2 },
                               { 1000, 21, null, 2 }};

        expected = new Object[][] {{ 1000, 20, null },
                                   { 1000, 20, null },
                                   { 1000, 21, null }};

        test(rows, 1, true, expected);

        // one pivot; no left rows
        rows = new Object[][]{ { 1000, 20, null, 0 }};
        expected = new Object[][] {};
        test(rows, 1, true, expected);

        // two pivots; simple case
        rows = new Object[][]{ { 1000, null, 100, 0 },
                               { 1000, 20, null, 2 },
                               { 1001, null, 101, 0 },
                               { 1001, 21, null, 2 }};

        expected = new Object[][]{{1000, 20, 100}, {1001, 21, 101}};

        test(rows, 1, true, expected);

        // multi pivots; no right rows
        rows = new Object[][]{ { 1000, 20, null, 2 },
                               { 1001, 20, null, 2 },
                               { 1002, 21, null, 2 }};

        expected = new Object[][] {{ 1000, 20, null },
                                   { 1001, 20, null },
                                   { 1002, 21, null }};

        test(rows, 1, true, expected);

        // multi pivots; no left rows
        rows = new Object[][]{ { 1000, 20, null, 0 },
                               { 1001, 20, null, 0 },
                               { 1002, 20, null, 0 }};
        expected = new Object[][] {};
        test(rows, 1, true, expected);

        // multipivots: hasleft - noleft - hasleft
        rows = new Object[][]{ { 1000, null, 20, 0 },
                               { 1000, 100, null, 2 },
                               { 1001, null, 20, 0 },
                               { 1002, null, 21, 0 },
                               { 1002, 101, null, 2 }};

        expected = new Object[][] {{1000, 100, 20}, {1002, 101, 21}};
        test(rows, 1, true, expected);

        // multipivots: hasleft - noright - hasleft
        rows = new Object[][]{ { 1000, null, 20, 0 },
                               { 1000, 100, null, 2 },
                               { 1001, null, null, 2 },
                               { 1002, null, 21, 0 },
                               { 1002, 101, null, 2 }};

        expected = new Object[][] {{1000, 100, 20}, { 1001, null, null }, {1002, 101, 21}};
        test(rows, 1, true, expected);
    }


    <T> Map<String, T> makeMap(String key, T value)
    {
        Map<String, T> map = new HashMap<String, T>();
        map.put(key, value);
        return map;
    }

//    public static void main(String[] args) throws InterruptedException, IOException, PreconditionException
//    {
//        new TestReduceJoin().testReduceInnerJoin();
////            char[] chars = new char[] {'\\',  'u', '0', '0', '1', 'a'};
////            String s = new String(chars);
////        System.out.println(s);
////        System.out.println(s.length());
//
//    }

}
