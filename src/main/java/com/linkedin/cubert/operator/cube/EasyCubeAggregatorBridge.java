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

package com.linkedin.cubert.operator.cube;

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.operator.AggregationBuffer;
import com.linkedin.cubert.utils.SchemaUtils;

/**
 * A bridge class that wraps user-defined aggregation functions (UDAFs) written with
 * {@link EasyCubeAggregator} interface into {@link DupleCubeAggregator} interface.
 * 
 * @author Maneesh Varshney
 * 
 */
public class EasyCubeAggregatorBridge implements DupleCubeAggregator
{
    private int outIndex;
    private Object reusedOutput;
    private final EasyCubeAggregator delegate;
    private AggregationBuffer[] buffer;

    public EasyCubeAggregatorBridge(EasyCubeAggregator delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public void setup(Block block, BlockSchema outputSchema, JsonNode json) throws IOException
    {
        BlockSchema inputSchema = block.getProperties().getSchema();
        BlockSchema aggSchema = outputSchema(inputSchema, json);
        String outColName = aggSchema.getColumnNames()[0];
        outIndex = outputSchema.getIndex(outColName);

        delegate.setup(inputSchema);
    }

    @Override
    public void allocate(int size)
    {
        buffer = new AggregationBuffer[size];
        for (int i = 0; i < size; i++)
        {
            buffer[i] = delegate.getAggregationBuffer();
        }
    }

    @Override
    public void processTuple(Tuple tuple) throws ExecException
    {
        delegate.processTuple(tuple);
    }

    @Override
    public void outputTuple(Tuple outputTuple, int index) throws ExecException
    {
        reusedOutput = delegate.output(reusedOutput, buffer[index]);
        outputTuple.set(outIndex, reusedOutput);
    }

    @Override
    public BlockSchema outputSchema(BlockSchema inputSchema, JsonNode json)
    {
        Schema pigSchema;
        try
        {
            pigSchema = SchemaUtils.convertFromBlockSchema(inputSchema);
            FieldSchema fieldSchema = delegate.outputSchema(pigSchema);

            return SchemaUtils.convertToBlockSchema(new Schema(fieldSchema));
        }
        catch (FrontendException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // return new PostCondition(delegate.outputSchema(inputSchema), null, null);
        return null;
    }

    @Override
    public void innerAggregate(int index)
    {
        delegate.aggregate(buffer[index]);
    }

    @Override
    public void aggregate(int index)
    {
        delegate.endMeasure(buffer[index]);
    }

}
