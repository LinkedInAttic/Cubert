package com.linkedin.cubert.examples;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.functions.Function;
import com.linkedin.cubert.operator.PreconditionException;

public class ArrayGetMinEntry extends Function
{

    @Override
    public Object eval(Tuple input) throws IOException
    {
        DataBag bag = (DataBag) input.get(0);
        int index = (Integer) input.get(1);

        if (bag == null)
            return null;

        Iterator<Tuple> it = bag.iterator();
        Tuple minTuple = null;
        Number minValue = null;

        while (it.hasNext())
        {
            Tuple tuple = it.next();
            Number field = (Number) tuple.get(index);
            if (field == null)
                continue;

            if (minTuple == null || minValue.doubleValue() > field.doubleValue())
            {
                minTuple = tuple;
                minValue = field;
            }
        }
        return minTuple;
    }

    @Override
    public ColumnType outputSchema(BlockSchema inputSchema) throws PreconditionException
    {
        BlockSchema tupleSchema =
                inputSchema.getColumnType(0)
                           .getColumnSchema()
                           .getColumnType(0)
                           .getColumnSchema();
        ColumnType type = new ColumnType("", DataType.TUPLE);
        type.setColumnSchema(tupleSchema);
        return type;
    }

}
