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
 * Concat takes a variable number of arguments and concatenates them into a simple string.
 * 
 * All arguments are converted to Strings (using .toString() method). null values are
 * treated as empty string (""). If all arguments are null, an empty string ("") is
 * returned.
 * 
 * @author Maneesh Varshney
 * 
 */
public class Concat extends Function
{
    private int nargs;

    @Override
    public Object eval(Tuple tuple) throws ExecException
    {
        String str = "";
        for (int i = 0; i < nargs; i++)
        {
            Object field = tuple.get(i);
            if (field != null)
                str = str + field.toString();
        }
        return str;
    }

    @Override
    public ColumnType outputSchema(BlockSchema inputSchema) throws PreconditionException
    {
        nargs = inputSchema.getNumColumns();
        if (nargs < 2)
            throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG,
                                            "CONCAT function requires atleast two arguments");

        return new ColumnType(null, DataType.STRING);
    }

}
