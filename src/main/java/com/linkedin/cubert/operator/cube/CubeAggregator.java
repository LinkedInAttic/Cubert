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
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.operator.PreconditionException;

/**
 * Interface that specifies aggregators for the CUBE operator.
 * <p>
 * Specifically, this aggregator applies for the cases where the grain of the data is same
 * as the grain of the CUBE. In other words, there are no "inner dimensions" that need to
 * be rolled up before computing the cube. See {@link DupleCubeAggregator} interface,
 * which allows rolling up of inner dimensions.
 * <p>
 * This aggregator is responsible for aggregating values for multiple value cuboids in the
 * cube. The total number of value cuboids that this aggregator will be asked to aggregate
 * is specified initially with the {@link allocate} method.
 * <p>
 * The {@code processTuple} method is called after each input tuple, and this aggregator
 * is expected to retrieve the relevant fields from the input tuple.
 * <p>
 * The {@link aggregate} method is called for each value cuboid. Each value cuboid is
 * uniquely indexed with an integer, which is provided to this method.
 * <p>
 * The {@link outputTuple} method is responsible for writing the aggregated value for a
 * given value cuboid into the provided output tuple.
 * <p>
 * Finally, the {@link outputSchema} must provide the schema of the output aggregate
 * fields. This method is called at the compile time, and is allowed to throw
 * {@link PreconditionException} which will result in compile time failure.
 * 
 * @see DupleCubeAggregator
 * 
 * @author Maneesh Varshney
 * 
 */
public interface CubeAggregator
{
    /**
     * Configure the aggregation operator.
     * 
     * @param block
     *            the block from where the tuple are generated
     * @param outputSchema
     *            the schema of output tuple
     * @param json
     *            a json node specifying the configuration properties.
     * @throws IOException
     */
    void setup(Block block, BlockSchema outputSchema, JsonNode json) throws IOException;

    /**
     * Indicates the maximum number of value cuboids this aggregator must handle.
     * 
     * @param size
     *            the maximum number of value cuboids to be handled by this aggregtor
     */
    void allocate(int size);

    /**
     * Clear the internal contents. This is called when the hash table is flushed, and built again with
     * more data.
     */
    void clear();

    /**
     * Handles the current input tuple. This aggregator is expected to retrieve the
     * relevant fields from the input tuple.
     * 
     * @param tuple
     *            the input tuple
     * @throws ExecException
     */
    void processTuple(Tuple tuple) throws ExecException;

    /**
     * Aggregate the currentValue for the value cuboid at the specified index
     * 
     * @param index
     *            the index of the value cuboid for which to aggregate
     */
    void aggregate(int index);

    /**
     * Write the final aggregated value for the specified value cuboid into the output
     * tuple.
     * 
     * @param outputTuple
     *            the tuple where the value is written
     * @param index
     *            the index of the value cuboid
     * @throws ExecException
     */
    void outputTuple(Tuple outputTuple, int index) throws ExecException;

    /**
     * Returns the schema of the output aggregate fields.
     * 
     * @param inputSchema
     *            schema of the input tuples
     * @param json
     *            the JSON configuration for this aggregator
     * @return the schema of the output aggregate fields
     * @throws PreconditionException
     */
    BlockSchema outputSchema(BlockSchema inputSchema, JsonNode json) throws PreconditionException;
}
