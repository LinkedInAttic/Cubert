package com.linkedin.cubert.operator.aggregate;

import java.io.IOException;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.ReadableInstant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.utils.TupleUtils;
import com.linkedin.cubert.utils.JsonUtils;

public class PresenceBitmapUDAF implements AggregationFunction
{
    private boolean debugOn = false;
	private int bitmap = 0;
	private final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
	private DateTime horizon;
	
    public PresenceBitmapUDAF(String horizonString)
    {
    	horizon = formatter.parseDateTime(horizonString);
    	System.out.println("PresenceBitmap UDAF constructor horizonString=" + horizonString);
    }

    public BagFactory mBagFactory = BagFactory.getInstance();
    private DataBag outputBag;

    private int inputColumnIndex;
    private int outputColumnIndex;
	

    @Override
    public void setup(Block block, BlockSchema outputSchema, JsonNode json) throws IOException
    {
        BlockSchema inputSchema = block.getProperties().getSchema();
        String[] inputColumnNames = JsonUtils.asArray(json, "input");
        assert (inputColumnNames.length == 1);
        String inputColumnName = inputColumnNames[0];
        inputColumnIndex = inputSchema.getIndex(inputColumnName);

        


        BlockSchema aggOutputSchema = outputSchema(inputSchema, json);
        String outputColumnName = aggOutputSchema.getName(0);

        outputColumnIndex = outputSchema.getIndex(outputColumnName);

        resetState();
    }

    @Override
    public void resetState()
    {
        bitmap = 0;
    }


    @Override
    public void aggregate(Tuple inputTuple) throws IOException
    {
    	String dateStr = (String) inputTuple.get(inputColumnIndex);

    	DateTime date = formatter.parseDateTime(dateStr);
        int diff = Days.daysBetween(horizon, date).getDays();
        if (debugOn)
          System.out.println("Tuple datestr = " + dateStr + " date diff = " + diff + " bitmap = " + bitmap);
        if (diff >= 0 && diff <= 31)
        {
            bitmap |= (1 << diff);
           
        }
    }

    @Override
    public void output(Tuple outputTuple) throws IOException
    {
        outputTuple.set(outputColumnIndex, bitmap);
        resetState();
    }

    @Override
    public void resetTuple(Tuple outputTuple) throws IOException
    {
        
    }

    @Override
    public BlockSchema outputSchema(BlockSchema inputSchema, JsonNode aggregateJson)
    {
        String inputColumnName = JsonUtils.asArray(aggregateJson, "input")[0];
        int inputColIndex = inputSchema.getIndex(inputColumnName);
        ColumnType inputColType = inputSchema.getColumnType(inputColIndex);

        String outputColumnName = "PresenceMap___" + inputColumnName;
        if (((ObjectNode) aggregateJson).get("output")  != null)
            outputColumnName = JsonUtils.getText(aggregateJson, "output");
        ColumnType outColType =
                new ColumnType(outputColumnName, DataType.INT);

        

        return new BlockSchema(new ColumnType[] { outColType });
    }
}
