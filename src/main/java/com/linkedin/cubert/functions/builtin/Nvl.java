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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.functions.Function;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.operator.PreconditionExceptionType;

/**
 * Builtin NVL function.
 * 
 * @author Maneesh Varshney
 * 
 */
public class Nvl extends Function
{
    @Override
    public Object eval(Tuple tuple) throws ExecException
    {
        Object o1 = tuple.get(0);
        return (o1 == null || (o1 instanceof String && ((String) o1).length()==0)? tuple.get(1) : o1);
    }

    @Override
    public ColumnType outputSchema(BlockSchema inputSchema) throws PreconditionException
    {
        if (inputSchema.getNumColumns() != 2)
        {
            throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG,
                    "NVL function takes exactly two arguments");
        }

        final ColumnType aColType = inputSchema.getColumnType(0);
        final ColumnType bColType = inputSchema.getColumnType(1);
        if (aColType.matches(bColType))
        {
            return aColType;
        }

        final DataType aType = aColType.getType();
        final DataType bType = bColType.getType();

        final DataType widerType = DataType.getWiderType(aType, bType);
        if (widerType == null)
        {
            String msg = String.format("The data types for NVL(%s, %s) are incompatible", aType, bType);
            throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA, msg);
        }
        return new ColumnType(null, widerType);
    }

}
