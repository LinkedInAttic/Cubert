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

package com.linkedin.cubert.block;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.linkedin.cubert.operator.TupleOperator;
import com.linkedin.cubert.plan.physical.PerfProfiler;
import com.linkedin.cubert.utils.TupleUtils;

/**
 * Similar to TupleOperatorBlock, but buffer the output tuples from operator for
 * performance profile.
 */
public class BufferedTupleOperatorBlock extends TupleOperatorBlock
{
    private static final int BUFFER_SIZE = 1000;
    private static final int NUM_BATCH_TO_REPORT = 100;

    protected Tuple[] outputBuffer;
    private int bufferSize;
    private int bufferPointer;
    private boolean lastBatch;

    private boolean shallowCopy;
    private boolean isOutputBlock;
    private int numBatchSinceLastReport; // Only valid for output block.
    private PerfProfiler profiler; // Only valid for output block.

    private LongWritable cumulativeElapsedTime;

    public BufferedTupleOperatorBlock(TupleOperator operator,
                                      BlockProperties props,
                                      LongWritable cumulativeElapsedTime)
    {
        super(operator, props);
        outputBuffer = new Tuple[BUFFER_SIZE];
        bufferSize = 0;
        bufferPointer = 0;
        lastBatch = false;
        this.cumulativeElapsedTime = cumulativeElapsedTime;

        shallowCopy = props.getSchema().allFieldsAllowShallowCopy();
        isOutputBlock = false;
        numBatchSinceLastReport = 0;
        profiler = null;

        // Pre-allocate the space for each tuple.
        int numCols = props.getSchema().getNumColumns();
        for (int i = 0; i < BUFFER_SIZE; i++)
            outputBuffer[i] = TupleFactory.getInstance().newTuple(numCols);
    }

    public void reset(TupleOperator operator)
    {
        this.operator = operator;

        bufferSize = 0;
        bufferPointer = 0;
        lastBatch = false;
    }

    public void setAsOutputBlock(PerfProfiler profiler)
    {
        isOutputBlock = true;
        this.profiler = profiler;
    }

    public void updatePerformanceCounter()
    {
        profiler.updatePerformanceCounter();
        numBatchSinceLastReport = 0;
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        if (bufferPointer < bufferSize)
        {
            // Output buffer is not exhausted, directly return the buffered tuple.
            Tuple nextTuple = outputBuffer[bufferPointer];
            bufferPointer++;
            return nextTuple;
        }

        // Avoid read operator.next() again.
        if (lastBatch)
            return null;

        // Output buffer reset.
        bufferPointer = 0;
        bufferSize = 0;

        long startNanoTime = System.nanoTime();
        // Fill up the buffer.
        for (int i = 0; i < BUFFER_SIZE; i++)
        {
            Tuple nextTuple = operator.next();
            if (nextTuple == null)
            {
                lastBatch = true;
                break;
            }

            if (shallowCopy)
                TupleUtils.copy(nextTuple, outputBuffer[i]);
            else
                TupleUtils.deepCopy(nextTuple, outputBuffer[i]);
            bufferSize++;
        }
        long endNanoTime = System.nanoTime();

        cumulativeElapsedTime.set(cumulativeElapsedTime.get() + endNanoTime
                - startNanoTime);
        if (isOutputBlock)
        {
            numBatchSinceLastReport++;
            if (numBatchSinceLastReport == NUM_BATCH_TO_REPORT)
                updatePerformanceCounter();
        }

        // End of block.
        if (bufferSize == 0)
            return null;

        // Return the first tuple in the buffer.
        bufferPointer++;
        return outputBuffer[0];
    }
}
