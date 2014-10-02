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

import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.functions.Function;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.operator.PreconditionExceptionType;

/**
 * Builtin SIZEOF function that returns the size of databags and maps.
 * 
 * @author Maneesh Varshney
 * 
 */
public class Sizeof extends Function
{

    @Override
    public Object eval(Tuple tuple) throws ExecException
    {
        Object field = tuple.get(0);

        if (field == null)
            return null;

        if (field instanceof DataBag)
            return ((DataBag) field).size();
        else if (field instanceof Map)
            return (long) ((Map) field).size();

        return null;
    }

    @Override
    public ColumnType outputSchema(BlockSchema inputSchema) throws PreconditionException
    {
        if (inputSchema.getNumColumns() != 1)
            throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG,
                                            "SIZEOF function takes exactly one argument");

        return new ColumnType(null, DataType.LONG);
    }

}
