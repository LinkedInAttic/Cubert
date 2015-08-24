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
 * Suite of built-in functions to cast primitive data types.
 * 
 * @author Maneesh Varshney
 * 
 */
public class Typecast extends Function
{
    private DataType inputType;
    private final DataType outputType;

    public Typecast(DataType type)
    {
        this.outputType = type;
    }

    @Override
    public Object eval(Tuple tuple) throws ExecException
    {
        Object val = tuple.get(0);
        if (val == null)
            return null;

        switch (inputType)
        {
        case BOOLEAN:
            return castBoolean((Boolean) val);
        case DOUBLE:
        case FLOAT:
        case INT:
        case LONG:
            return castNumber((Number) val);
        case STRING:
            return castString((String) val);
        default:
            break;

        }

        return null;
    }

    private Object castNumber(Number val)
    {
        switch (outputType)
        {
        case DOUBLE:
            return val.doubleValue();
        case FLOAT:
            return val.floatValue();
        case INT:
            return val.intValue();
        case LONG:
            return val.longValue();
        case STRING:
            return val.toString();
        default:
            break;
        }
        return null;
    }

    private Object castString(String val)
    {
        switch (outputType)
        {
        case DOUBLE:
            return Double.parseDouble(val);
        case FLOAT:
            return Float.parseFloat(val);
        case INT:
            return Integer.parseInt(val);
        case LONG:
            return Long.parseLong(val);
        case STRING:
            return val;
        default:
            break;
        }
        return null;
    }

    private Object castBoolean(Boolean val) {
        if (val == null)
            return null;

        switch (outputType) {
            case DOUBLE:
                return val ? 1.0d : 0.0d;
            case FLOAT:
                return val ? 1.0f : 0.0f;
            case INT:
                return val ? 1 : 0;
            case LONG:
                return val ? 1L : 0L;
            case STRING:
                return val.toString();
            default:
                break;
        }

        return null;
    }

    @Override
    public ColumnType outputSchema(BlockSchema inputSchema) throws PreconditionException
    {
        if (inputSchema.getNumColumns() != 1)
            throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG,
                                            "Typecast function takes exactly one argument");

        inputType = inputSchema.getColumnType(0).getType();
        if (!inputType.isPrimitive())
            throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG,
                                            "Typecast function can be applied to primitive data types only");

        return new ColumnType(null, outputType);
    }

}
