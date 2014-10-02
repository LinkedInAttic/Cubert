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

package com.linkedin.cubert.operator.aggregate;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.operator.PreconditionException;

/**
 * Specifies a function to aggregate a column.
 * 
 * This function is used in conjunction with the GROUP_BY operator as well as in the
 * Combiner operation.
 * 
 * @author Maneesh Varshney
 * 
 */
public interface AggregationFunction
{
    /**
     * Configure the aggregation function.
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
     * Resets the state of the aggregator.
     */
    void resetState();

    /**
     * Aggregate the specified tuple.
     * 
     * @param input
     * @throws IOException
     */
    void aggregate(Tuple input) throws IOException;

    /**
     * Write the aggregated result to the specified output tuple.
     * 
     * @param output
     * @throws IOException
     */
    void output(Tuple output) throws IOException;

    /**
     * Result the content of the aggregation column in the output tuple.
     * 
     * @param output
     */
    void resetTuple(Tuple output) throws IOException;

    /**
     * Returns the schema of the output.
     * 
     * @param inputSchema
     * @param json
     * @return
     * @throws PreconditionException
     */
    BlockSchema outputSchema(BlockSchema inputSchema, JsonNode json) throws PreconditionException;

}
