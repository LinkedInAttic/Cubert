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

import org.apache.pig.data.Tuple;

import com.linkedin.cubert.operator.cube.EasyCubeAggregator;

/**
 * 
 * Adapter method for efficient storage and handing of UDAF operators
 * 
 * @author Mani Parkhe
 */
public class UDAFOperatorAdapter
{
    private AggregationBuffer[] aggregations = null;
    private EasyCubeAggregator udaf = null;

    public UDAFOperatorAdapter(EasyCubeAggregator udafOp)
    {
        this.udaf = udafOp;
    }

    public void allocate(int size)
    {
        if (null != aggregations)
            throw new RuntimeException();

        aggregations = new AggregationBuffer[size];
        for (int i = 0; i < size; ++i)
            aggregations[i] = udaf.getAggregationBuffer();
    }

    public void aggregate(int index) throws IOException
    {
        udaf.aggregate(aggregations[index]);
    }

    public void endMeasure(int index)
    {
        udaf.endMeasure(aggregations[index]);
    }

    public void output(Tuple output, int index) throws Exception
    {
        udaf.output(output, aggregations[index]);
    }

    public void processTuple(Tuple inputTuple) throws Exception
    {
        udaf.processTuple(inputTuple);
    }
}
