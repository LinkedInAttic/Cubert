package com.linkedin.cubert.operator;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.operator.aggregate.AggregationFunction;
import com.linkedin.cubert.operator.aggregate.MaxAggregation;
import com.linkedin.cubert.operator.aggregate.MinAggregation;
import com.linkedin.cubert.operator.aggregate.SumAggregation;
import java.io.IOException;
import java.util.Arrays;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Mani Parkhe
 */
public class TestAggregators
{
    private Tuple runAgg(Object[][] input, AggregationFunction agg, DataType outputType)
        throws IOException, InterruptedException
    {
        Block dataBlock =
            new ArrayBlock(Arrays.asList(input), new String[] { "value" });

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("input", "value");
        node.put("output", "agg");

        agg.setup(dataBlock, new BlockSchema(outputType.toString() + " agg"), node);

        agg.resetState();

        Tuple inputTuple;
        while ((inputTuple = dataBlock.next()) != null)
        {
            agg.aggregate(inputTuple);
        }

        Tuple outputTuple = TupleFactory.getInstance().newTuple(1);
        agg.output(outputTuple);

        return outputTuple;
    }

    @Test
    public void testNullAgg()
        throws IOException, InterruptedException
    {
        Object[][] dataRows = { { null }, {null}, {null}, {null} };

        Tuple outputTuple = runAgg(dataRows, new MinAggregation(), DataType.LONG);
        Assert.assertEquals(outputTuple.get(0), null);

        outputTuple = runAgg(dataRows, new MaxAggregation(), DataType.LONG);
        Assert.assertEquals(outputTuple.get(0), null);

        outputTuple = runAgg(dataRows, new SumAggregation(), DataType.LONG);
        Assert.assertEquals(outputTuple.get(0), null);
    }

    @Test
    public void testIntegerMinAgg()
        throws IOException, InterruptedException
    {
        Object[][] dataRows = { { 100 }, { 100 }, { null },  { 101 } , { 100 }, { 0 }, { 1 }, { -1 }};

        Tuple outputTuple = runAgg(dataRows, new MinAggregation(), DataType.INT);

        Assert.assertEquals(((Integer) outputTuple.get(0)).intValue(), -1);
    }

    @Test
    public void testDoubleMinAgg()
        throws IOException, InterruptedException
    {
        Object[][] dataRows = { { 100.0d }, { 100.0d }, { -101.1d } , { 100.0d }, { 0.000000001d }, { null }, { 1.0d }, { -0.01d }};

        Tuple outputTuple = runAgg(dataRows, new MinAggregation(), DataType.DOUBLE);

        Assert.assertEquals(((Double) outputTuple.get(0)).doubleValue(), -101.1d);
    }

    @Test
    public void testIntegerMaxAgg()
        throws IOException, InterruptedException
    {
        Object[][] dataRows = { { 1 }, { null }, { 1 }, { 1 } , { 0 }, { -1 }, { 2 }, { -1000 }};

        Tuple outputTuple = runAgg(dataRows, new MaxAggregation(), DataType.INT);

        Assert.assertEquals(((Integer) outputTuple.get(0)).intValue(), 2);
    }

    @Test
    public void testDoubleMaxAgg()
        throws IOException, InterruptedException
    {
        Object[][] dataRows = { { 100.0d }, { 100.1d }, { null }, { -101.1d } , { 100.0d }, { 100.001d }, { 1.0d }, { -0.01d }};

        Tuple outputTuple = runAgg(dataRows, new MaxAggregation(), DataType.DOUBLE);

        Assert.assertEquals(((Double) outputTuple.get(0)).doubleValue(), 100.1d);
    }
}
