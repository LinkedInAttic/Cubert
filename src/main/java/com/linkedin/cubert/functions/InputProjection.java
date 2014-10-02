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

import org.apache.pig.data.Tuple;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.operator.PreconditionException;

/**
 * A function that selects a column from the input tuple.
 * <p>
 * This function selects column from the "root" tuple (that is, this function is not used
 * to select from nested tuples).
 * <p>
 * This function caches the column for each input tuple and the {@code eval} method
 * returns the same cached value each time it is invoked.
 * 
 * @author Maneesh Varshney
 * 
 */
public class InputProjection extends Function
{
    private final int selector;
    private Object cachedValue;

    public InputProjection(int selector)
    {
        this.selector = selector;
    }

    /**
     * Called by the FunctionTree to set the column object for a tuple. While the
     * FunctionTree is evaluating for the same tuple, this function returns the same
     * cached value.
     * 
     * @param value
     */
    public void setValue(Object value)
    {
        cachedValue = value;
    }

    @Override
    public Object eval(Tuple tuple)
    {
        return cachedValue;
    }

    @Override
    public ColumnType outputSchema(BlockSchema inputSchema) throws PreconditionException
    {
        return inputSchema.getColumnType(selector);
    }

}
