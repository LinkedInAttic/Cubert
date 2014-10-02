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

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.operator.AggregationBuffer;

/**
 * Generic interface for all cube aggregators.
 * 
 * This relies on an external storage type <code>AggregationBuffer</code> which stores the
 * partial and complete results of computation. Implementing classes would need to provide
 * and API to return a fresh object (on demand) of the storage type extended on
 * <code>AggregationBuffer</code>
 * 
 * @author Mani Parkhe
 */

public interface EasyCubeAggregator
{
    /*
     * setup aggregator. This method will be called only ONCE in the entire flow per
     * instance of this aggregator.
     * 
     * The objective would be to save indexes of columns in <code>inputSchema</code> and
     * other meta data.
     */
    public void setup(BlockSchema inputSchema) throws FrontendException;

    /*
     * This method will me called ONCE per <code>inputTuple</code>.
     * 
     * The objective would be to save/update shared state.
     */
    public void processTuple(Tuple inputTuple) throws ExecException;

    /*
     * Main aggregation operation.
     * 
     * Will be called MULTIPLE times for each <code>inputTuple</code> for each qualifying
     * combination of dimensions based on # grouping sets.
     */
    public void aggregate(AggregationBuffer aggregationBuffer);

    /*
     * Called ONCE on each <code>AggregationBuffer</code> object that is updated during
     * the processing of the current measure.
     */
    public void endMeasure(AggregationBuffer aggregationBuffer);

    /*
     * Returns a fresh copy of storage class extended from <code>AggreagtionBuffer</code>.
     */
    public AggregationBuffer getAggregationBuffer();

    /*
     * Publish output schema for this aggregation.
     */
    public FieldSchema outputSchema(Schema inputSchema) throws IOException;

    /*
     * Extract output data from aggregation buffer and output it as described as in
     * <code>FieldSchema</code>
     * 
     * @parameter : <code>reUsedOutput</code> is an optimization should the user chose to
     * re-use the output object. First call to <code>output</code> method would send a
     * <code>null</code> for <code>reUsedOutput</code>. This method should check for this
     * condition and allocate an object and return it to caller as return argument. Future
     * calls to this method would re-send this previously returned object.
     */
    public Object output(Object reUsedOutput, AggregationBuffer aggregationBuffer) throws ExecException;

}
