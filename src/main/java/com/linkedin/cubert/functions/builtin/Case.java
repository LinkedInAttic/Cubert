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
 * The builtin CASE function.
 * 
 * @author Maneesh Varshney
 * 
 */
public class Case extends Function
{
    private int numCases;

    @Override
    public Object eval(Tuple tuple) throws ExecException
    {
        for (int i = 0; i < numCases; i++)
        {
            Boolean condition = (Boolean) tuple.get(2 * i);

            if (condition != null && condition)
                return tuple.get(2 * i + 1);
        }
        return null;
    }

    @Override
    public ColumnType outputSchema(BlockSchema inputSchema) throws PreconditionException
    {
        numCases = inputSchema.getNumColumns() / 2;
        if (numCases == 0)
        {
            throw new PreconditionException(PreconditionExceptionType.MISC_ERROR,
                                            "Malformed CASE statement: Must have at least 1 case to execute");
        }

        final ColumnType[] colTypes = new ColumnType[numCases];

        boolean allSameTypes = true;

        for (int i = 0; i < numCases; i++)
        {
            colTypes[i] = inputSchema.getColumnType(2 * i + 1);
            /* check if all the data types are identical */
            if (!colTypes[0].matches(colTypes[i]))
            {
                allSameTypes = false;
            }
        }

        if (allSameTypes)
        {
            return colTypes[0];
        }

        // if not all types are same, then find the widest type
        DataType outType = colTypes[0].getType();
        for (int i = 1; i < numCases; i++)
        {
            DataType widerType = DataType.getWiderType(outType, colTypes[i].getType());
            if (widerType == null)
            {
                String msg =
                        String.format("Incompatible data types in CASE function: %s and %s",
                                      outType,
                                      colTypes[i]);
                throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA,
                                                msg);
            }
            outType = widerType;
        }
        return new ColumnType(null, outType);
    }
}
