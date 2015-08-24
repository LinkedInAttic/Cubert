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

package com.linkedin.cubert.functions;

import java.io.IOException;
import java.util.List;

import org.apache.pig.data.Tuple;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.operator.PreconditionException;

/**
 * Abstract class for defining user-defined functions for the GENERATE operator.
 * <p>
 * The UDF class is responsible for:
 * <ul>
 * <li>generating the output column type ({@code getCacheFile} method).</li>
 * 
 * <li>evaluating the input tuple and generating the output ({@code eval} method)</li>
 * 
 * <li>[Optionally] listing the files that must be stored in the distributed cache (
 * {@code getCacheFiles} method)</li>
 * </ul>
 * <p>
 * The UDF has access to the current block using the protected {@code setBlock} method.
 * <p>
 * Note: the {@code outputSchema} and {@code getCacheFiles} methods will be called at the
 * compile time, as well as the run time.
 * <p>
 * At run time, the {@code outputSchema} method is called before calling the {@code eval}
 * method (so it okay to initialize object variables in the outputSchema method).
 * 
 * @author Maneesh Varshney
 * 
 */
public abstract class Function
{
    /**
     * Optionally override this method to perform setup operations.
     *
     * @param block
     */
    protected void setBlock(Block block) throws PreconditionException
    {

    }

    /**
     * Evaluate the input tuple to generate output object.
     * 
     * @param tuple
     *            the input tuple
     * @return evaluated output
     * @throws IOException
     */
    public abstract Object eval(Tuple tuple) throws IOException;

    /**
     * Generate the ColumnType of the output.
     * 
     * @param inputSchema
     *            Schema of the input tuple
     * @return Output object ColumnType
     * @throws PreconditionException
     */
    public abstract ColumnType outputSchema(BlockSchema inputSchema) throws PreconditionException;

    /**
     * Returns a list of files that must be stored in distributed cache.
     * 
     * @return a list of file names
     */
    public List<String> getCacheFiles()
    {
        return null;
    }
}
