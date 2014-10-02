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

package com.linkedin.cubert.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.operator.AggregationBuffer;
import com.linkedin.cubert.operator.cube.EasyCubeAggregator;

/**
 *
 */
public class ExampleEasyCubeAggregator implements EasyCubeAggregator
{

    class myAggregator extends AggregationBuffer
    {
        long sum = 0;
        long sumSq = 0;

        public void aggregate(int val)
        {
            sum += val;
            sumSq += (val * val);
        }

        public Object getSum()
        {
            return sum;
        }

        public Object getSumSq()
        {
            return sumSq;
        }

        public String toString()
        {
            return new String("sum =" + sum + " sum_sq = " + sumSq);
        }
    }

    private int inputColumnIndex;
    private int currVal = 0;
    private final int sumIndex = 0;
    private final int sumSqIndex = 1;

    @Override
    public void setup(BlockSchema inputSchema) throws FrontendException
    {
        inputColumnIndex = inputSchema.getIndex("memberId");
    }

    @Override
    public void processTuple(Tuple inputTuple) throws ExecException
    {
        currVal = ((Integer) inputTuple.get(inputColumnIndex)).intValue();
    }

    @Override
    public void aggregate(AggregationBuffer aggregationBuffer)
    {
        ((myAggregator) aggregationBuffer).aggregate(currVal);
    }

    @Override
    public void endMeasure(AggregationBuffer aggregationBuffer)
    {
        /* nothing to do */
    }

    @Override
    public AggregationBuffer getAggregationBuffer()
    {
        return new myAggregator();
    }

    @Override
    public FieldSchema outputSchema(Schema inputSchema) throws IOException
    {
        List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
        fieldSchemas.add(new FieldSchema("sum", DataType.LONG));
        fieldSchemas.add(new FieldSchema("sum_squared", DataType.LONG));
        Schema nestedTupleSchema = new Schema(fieldSchemas);

        return new FieldSchema("resultsTuple", nestedTupleSchema, DataType.TUPLE);
    }

    @Override
    public Object output(Object reUsedOutput, AggregationBuffer aggregationBuffer) throws ExecException
    {
        Tuple resultTuple = (Tuple) reUsedOutput;
        if (resultTuple == null)
        {
            TupleFactory mTupleFactory = TupleFactory.getInstance();
            resultTuple = mTupleFactory.newTuple(2);
        }
        resultTuple.set(sumIndex, ((myAggregator) aggregationBuffer).getSum());
        resultTuple.set(sumSqIndex, ((myAggregator) aggregationBuffer).getSumSq());
        return resultTuple;
    }
}
