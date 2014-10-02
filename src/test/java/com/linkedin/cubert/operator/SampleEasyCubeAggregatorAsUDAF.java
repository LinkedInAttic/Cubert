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
public class SampleEasyCubeAggregatorAsUDAF implements EasyCubeAggregator
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

    /**
     * {@inheritDoc}
     * 
     * @throws Exception
     * @see com.linkedin.cubert.operator.cube.EasyCubeAggregator#setup(com.linkedin.cubert.block.Block,
     *      com.linkedin.cubert.block.BlockSchema, org.codehaus.jackson.JsonNode)
     */
    @Override
    public void setup(BlockSchema inputSchema) throws FrontendException
    {
        inputColumnIndex = inputSchema.getIndex("memberId");
    }

    /**
     * {@inheritDoc}
     * 
     * @throws Exception
     * @throws Exception
     * @see com.linkedin.cubert.operator.cube.EasyCubeAggregator#processTuple(org.apache.pig.data.Tuple)
     */
    @Override
    public void processTuple(Tuple inputTuple) throws ExecException
    {
        currVal = ((Integer) inputTuple.get(inputColumnIndex)).intValue();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.linkedin.cubert.operator.cube.EasyCubeAggregator#aggregate(com.linkedin.cubert.operator.AggregationBuffer)
     */
    @Override
    public void aggregate(AggregationBuffer aggregationBuffer)
    {
        ((myAggregator) aggregationBuffer).aggregate(currVal);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.linkedin.cubert.operator.cube.EasyCubeAggregator#endMeasure(com.linkedin.cubert.operator.AggregationBuffer)
     */
    @Override
    public void endMeasure(AggregationBuffer aggregationBuffer)
    {
        /* nothing to do */
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.linkedin.cubert.operator.cube.EasyCubeAggregator#getAggregationBuffer()
     */
    @Override
    public AggregationBuffer getAggregationBuffer()
    {
        return new myAggregator();
    }

    /**
     * {@inheritDoc}
     * 
     * @throws FrontendException
     * @see com.linkedin.cubert.operator.cube.EasyCubeAggregator#outputSchema(org.apache.pig.impl.logicalLayer.schema.Schema)
     */
    @Override
    public FieldSchema outputSchema(Schema inputSchema) throws IOException
    {
        List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
        fieldSchemas.add(new FieldSchema("sum", DataType.LONG));
        fieldSchemas.add(new FieldSchema("sum_squared", DataType.LONG));
        Schema nestedTupleSchema = new Schema(fieldSchemas);

        return new FieldSchema("resultsTuple", nestedTupleSchema, DataType.TUPLE);
    }

    /**
     * {@inheritDoc}
     * 
     * @throws Exception
     * @see com.linkedin.cubert.operator.cube.EasyCubeAggregator#output(org.apache.pig.data.Tuple,
     *      com.linkedin.cubert.operator.AggregationBuffer)
     */
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
