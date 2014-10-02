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

package com.linkedin.cubert.operator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.utils.TupleUtils;

public class MedianFlattenOperator implements TupleOperator
{
    private Block block;
    private BlockSchema schema;
    private Tuple secondOutput;

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        block = input.values().iterator().next();

        schema = getOutputSchema(block.getProperties().getSchema());

        secondOutput = null;
    }

    private BlockSchema getOutputSchema(BlockSchema inputSchema)
    {

        int sizeOfOutputSchema = inputSchema.getColumnNames().length + 1;
        ColumnType[] columns = new ColumnType[sizeOfOutputSchema];

        for (int i = 0; i < inputSchema.getColumnNames().length; i++)
        {
            ColumnType column = inputSchema.getColumnType(i);
            if (column.getType() != DataType.BAG)
            {
                columns[i] = column;
            }
            else
            {
                ColumnType tuple = column.getColumnSchema().getColumnType(0);

                ColumnType[] innerColumns = tuple.getColumnSchema().getColumnTypes();
                assert (innerColumns.length == 2);

                columns[i] = innerColumns[0];
                columns[i + 1] = innerColumns[1];
            }
        }
        return new BlockSchema(columns);
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        if (secondOutput != null)
        {
            Tuple copy = TupleUtils.getDeepCopy(secondOutput);
            secondOutput = null;
            return copy;
        }

        Tuple t = block.next();
        if (t == null)
        {
            return null;
        }

        // tupleFlatten also sets secondOutput if the bag contains more than one tuple
        return tupleFlatten(t);
    }

    private Tuple tupleFlatten(Tuple inTuple) throws ExecException
    {
        int outputSchemaSize = schema.getNumColumns();
        Tuple outTuple = TupleFactory.getInstance().newTuple(outputSchemaSize);

        // last column of inTuple is bag
        for (int i = 0; i < inTuple.size() - 1; i++)
        {
            outTuple.set(i, inTuple.get(i));
        }

        // outputSchemaSize is 1 greater than inputSchemaSize, and tuple zero indexed, so
        // -2
        DataBag bag = (DataBag) inTuple.get(outputSchemaSize - 2);
        Iterator<Tuple> bagIterator = bag.iterator();
        Tuple firstTuple = bagIterator.next();

        if (firstTuple == null)
        {
            throw new RuntimeException("Bag should not be empty");
        }

        outTuple.set(outputSchemaSize - 2, firstTuple.get(0));
        outTuple.set(outputSchemaSize - 1, firstTuple.get(1));

        if (bagIterator.hasNext())
        {
            Tuple secondTuple = bagIterator.next();
            secondOutput = TupleFactory.getInstance().newTuple(outputSchemaSize);

            // last column of inTuple is bag
            for (int i = 0; i < inTuple.size() - 1; i++)
            {
                secondOutput.set(i, inTuple.get(i));
            }

            secondOutput.set(outputSchemaSize - 2, secondTuple.get(0));
            secondOutput.set(outputSchemaSize - 1, secondTuple.get(1));
        }

        return outTuple;

        /*
         * Tuple outTuple = TupleFactory.getInstance().newTuple(4);
         * 
         * outTuple.set(0, inTuple.get(0)); outTuple.set(1, inTuple.get(1));
         * 
         * DataBag bag = (DataBag) inTuple.get(2); Iterator<Tuple> bagIterator =
         * bag.iterator(); Tuple firstTuple = bagIterator.next();
         * 
         * if (firstTuple == null) { throw new RuntimeException
         * ("Bag should not be empty"); }
         * 
         * outTuple.set(2, firstTuple.get(0)); outTuple.set(3, firstTuple.get(1));
         * 
         * // case of two outputs if (bagIterator.hasNext()) { Tuple secondTuple =
         * bagIterator.next(); secondOutput = TupleFactory.getInstance().newTuple(4);
         * secondOutput.set(0, inTuple.get(0)); secondOutput.set(1, inTuple.get(1));
         * secondOutput.set(2, secondTuple.get(0)); secondOutput.set(3,
         * secondTuple.get(1)); }
         * 
         * return outTuple;
         */

    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        PostCondition condition = preConditions.values().iterator().next();
        BlockSchema inputSchema = condition.getSchema();
        return new PostCondition(getOutputSchema(inputSchema),
                                 condition.getPartitionKeys(),
                                 condition.getSortKeys());

    }

}
