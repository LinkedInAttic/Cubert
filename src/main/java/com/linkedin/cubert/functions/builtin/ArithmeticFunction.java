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
 * Suite of built-in arithmetic functions.
 * <p>
 * If the inputs are numerical types, these builtin functions will generate output in the
 * wider data type. If any of the input is null, these functions will output null.
 * <p>
 * Following functions are implemented:
 * <ul>
 * <li>{@literal +}</li>
 * <li>{@literal -}</li>
 * <li>{@literal /}</li>
 * <li>{@literal *}</li>
 * <li>{@literal %}</li>
 * <li>{@literal <<}</li>
 * </ul>
 * 
 * @author Maneesh Varshney
 * 
 */
public class ArithmeticFunction extends Function
{
    private final FunctionType type;
    private DataType outputType;

    public ArithmeticFunction(FunctionType type)
    {
        this.type = type;
    }

    @Override
    public Object eval(Tuple tuple) throws ExecException
    {

        Object o1 = tuple.get(0);
        Object o2 = tuple.get(1);
        if (o1 == null || o2 == null)
            return null;

        switch (type)
        {
        case ADD:
            return add(o1, o2);
        case DIVIDE:
            return divide(o1, o2);
        case MINUS:
            return minus(o1, o2);
        case MOD:
            return mod(o1, o2);
        case TIMES:
            return times(o1, o2);
        case LSHIFT:
            return lshift(o1, o2);
        case RSHIFT:
            return rshift(o1, o2);
        default:
            break;
        }
        return null;
    }

    private Object add(Object o1, Object o2)
    {
        switch (outputType)
        {
        case DOUBLE:
            return ((Number) o1).doubleValue() + ((Number) o2).doubleValue();
        case FLOAT:
            return ((Number) o1).floatValue() + ((Number) o2).floatValue();
        case INT:
            return ((Number) o1).intValue() + ((Number) o2).intValue();
        case LONG:
            return ((Number) o1).longValue() + ((Number) o2).longValue();
        default:
            break;
        }
        return null;
    }

    private Object minus(Object o1, Object o2)
    {
        switch (outputType)
        {
        case DOUBLE:
            return ((Number) o1).doubleValue() - ((Number) o2).doubleValue();
        case FLOAT:
            return ((Number) o1).floatValue() - ((Number) o2).floatValue();
        case INT:
            return ((Number) o1).intValue() - ((Number) o2).intValue();
        case LONG:
            return ((Number) o1).longValue() - ((Number) o2).longValue();
        default:
            break;
        }
        return null;
    }

    private Object times(Object o1, Object o2)
    {
        switch (outputType)
        {
        case DOUBLE:
            return ((Number) o1).doubleValue() * ((Number) o2).doubleValue();
        case FLOAT:
            return ((Number) o1).floatValue() * ((Number) o2).floatValue();
        case INT:
            return ((Number) o1).intValue() * ((Number) o2).intValue();
        case LONG:
            return ((Number) o1).longValue() * ((Number) o2).longValue();
        default:
            break;
        }
        return null;
    }

    private Object divide(Object o1, Object o2)
    {
        switch (outputType)
        {
        case DOUBLE:
            return ((Number) o1).doubleValue() / ((Number) o2).doubleValue();
        case FLOAT:
            return ((Number) o1).floatValue() / ((Number) o2).floatValue();
        case INT:
            return ((Number) o1).intValue() / ((Number) o2).intValue();
        case LONG:
            return ((Number) o1).longValue() / ((Number) o2).longValue();
        default:
            break;
        }
        return null;
    }

    private Object mod(Object o1, Object o2)
    {
        switch (outputType)
        {
        case INT:
            return ((Number) o1).intValue() % ((Number) o2).intValue();
        case LONG:
            return ((Number) o1).longValue() % ((Number) o2).longValue();
        default:
            break;
        }
        return null;
    }

    private Object rshift(Object o1, Object o2)
    {
        switch (outputType)
        {
        case INT:
            return ((Number) o1).intValue() >>> ((Number) o2).intValue();
        case LONG:
            return ((Number) o1).longValue() >>> ((Number) o2).longValue();
        default:
            break;
        }
        return null;
    }

    private Object lshift(Object o1, Object o2)
    {
        switch (outputType)
        {
        case INT:
            return ((Number) o1).intValue() << ((Number) o2).intValue();
        case LONG:
            return ((Number) o1).longValue() << ((Number) o2).longValue();
        default:
            break;
        }
        return null;
    }

    @Override
    public ColumnType outputSchema(BlockSchema inputSchema) throws PreconditionException
    {
        ColumnType type1 = inputSchema.getColumnType(0);
        ColumnType type2 = inputSchema.getColumnType(1);

        if ((type == FunctionType.LSHIFT || type == FunctionType.MOD || type == FunctionType.RSHIFT)
                && (!type1.getType().isIntOrLong() || !type2.getType().isIntOrLong()))
            throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA,
                                            "The LHS and RHS of " + type
                                                    + " function must be int or long");

        if (!type1.getType().isNumerical())
            throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA,
                                            "The LHS of " + type
                                                    + " function is not numerical");
        if (!type2.getType().isNumerical())
            throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA,
                                            "The RHS of " + type
                                                    + " function is not numerical");

        outputType = DataType.getWiderType(type1.getType(), type2.getType());
        return new ColumnType(null, outputType);
    }
}
