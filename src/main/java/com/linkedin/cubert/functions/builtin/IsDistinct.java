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

package com.linkedin.cubert.functions.builtin;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.functions.Function;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.operator.PreconditionExceptionType;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Function that uses a HashSet to check if the input column has been seen before.
 *
 * @author Maneesh Varshney
 */
public class IsDistinct extends Function
{
    private final Set<Object> set = new HashSet<Object>();

    @Override
    public Object eval(Tuple tuple) throws IOException
    {
        Object obj = tuple.get(0);
        boolean isDistinct = !set.contains(obj);

        if (isDistinct)
            set.add(obj);

        return isDistinct;
    }

    @Override
    public ColumnType outputSchema(BlockSchema inputSchema) throws PreconditionException
    {
        if (inputSchema.getNumColumns() != 1)
        {
            throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA,
                                            "IsDistinct function takes one argument only");
        }

        return new ColumnType("", DataType.BOOLEAN);
    }
}
