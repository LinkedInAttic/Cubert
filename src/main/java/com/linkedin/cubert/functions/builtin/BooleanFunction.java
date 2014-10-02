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

import java.util.HashSet;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.functions.Function;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.operator.PreconditionExceptionType;

/**
 * A suite of builtin boolean functions.
 * <p>
 * If any of the input is null, then output will also be null (except IS NULL, IS NOT NULL
 * and IN).
 * 
 * 
 * @author Maneesh Varshney
 * 
 */
public class BooleanFunction extends Function
{
    private final FunctionType type;
    private DataType widerType;
    private Set<Object> valueSet = null;

    public BooleanFunction(FunctionType type)
    {
        this.type = type;
    }

    @Override
    public Object eval(Tuple tuple) throws ExecException
    {
        Object o1 = tuple.get(0);
        Object o2;

        switch (type)
        {
        case NOT:
            return o1 == null ? null : !(Boolean) o1;
        case IS_NULL:
            return o1 == null;
        case IS_NOT_NULL:
            return o1 != null;
        case IN:
            // if input is null, we return false
            if (o1 == null)
                return false;
            if (valueSet == null)
            {
                valueSet = new HashSet<Object>();
                for (int i = 1; i < tuple.size(); i++)
                    valueSet.add(tuple.get(i));
            }
            return valueSet.contains(o1);
        case EQ:
            return eq(o1, tuple.get(1));
        case NE:
            Boolean val = eq(o1, tuple.get(1));
            return val == null ? null : !val;
        case AND:
            if (o1 == null || !((Boolean) o1))
                return false;
            o2 = tuple.get(1);
            return o2 == null ? false : (Boolean) o2;
        case OR:
            if (o1 != null && ((Boolean) o1))
                return true;

            // o1 is null or false
            o2 = tuple.get(1);
            return o2 == null ? false : (Boolean) o2;
        case LT:
            return lt(o1, tuple.get(1));
        case LE:
            return le(o1, tuple.get(1));
        case GT:
            return gt(o1, tuple.get(1));
        case GE:
            return ge(o1, tuple.get(1));

        default:
            break;
        }

        return null;
    }

    private Boolean eq(Object o1, Object o2)
    {
        if (o1 == null || o2 == null)
            return null;

        if (widerType != null)
        {
            switch (widerType)
            {
            case DOUBLE:
                return ((Number) o1).doubleValue() == ((Number) o2).doubleValue();
            case FLOAT:
                return ((Number) o1).floatValue() == ((Number) o2).floatValue();
            case INT:
                return ((Number) o1).intValue() == ((Number) o2).intValue();
            case LONG:
                return ((Number) o1).longValue() == ((Number) o2).longValue();
            default:
                break;
            }
        }
        return o1.equals(o2);
    }

    private Boolean lt(Object o1, Object o2)
    {
        if (o1 == null || o2 == null)
            return null;

        switch (widerType)
        {
        case DOUBLE:
            return ((Number) o1).doubleValue() < ((Number) o2).doubleValue();
        case FLOAT:
            return ((Number) o1).floatValue() < ((Number) o2).floatValue();
        case INT:
            return ((Number) o1).intValue() < ((Number) o2).intValue();
        case LONG:
            return ((Number) o1).longValue() < ((Number) o2).longValue();
        case STRING:
            return ((String) o1).compareTo(((String) o2)) < 0;
        default:
            break;
        }
        return null;
    }

    private Object le(Object o1, Object o2)
    {
        if (o1 == null || o2 == null)
            return null;

        switch (widerType)
        {
        case DOUBLE:
            return ((Number) o1).doubleValue() <= ((Number) o2).doubleValue();
        case FLOAT:
            return ((Number) o1).floatValue() <= ((Number) o2).floatValue();
        case INT:
            return ((Number) o1).intValue() <= ((Number) o2).intValue();
        case LONG:
            return ((Number) o1).longValue() <= ((Number) o2).longValue();
        case STRING:
            return ((String) o1).compareTo(((String) o2)) <= 0;
        default:
            break;
        }
        return null;
    }

    private Object gt(Object o1, Object o2)
    {
        if (o1 == null || o2 == null)
            return null;

        switch (widerType)
        {
        case DOUBLE:
            return ((Number) o1).doubleValue() > ((Number) o2).doubleValue();
        case FLOAT:
            return ((Number) o1).floatValue() > ((Number) o2).floatValue();
        case INT:
            return ((Number) o1).intValue() > ((Number) o2).intValue();
        case LONG:
            return ((Number) o1).longValue() > ((Number) o2).longValue();
        case STRING:
            return ((String) o1).compareTo(((String) o2)) > 0;
        default:
            break;
        }
        return null;
    }

    private Object ge(Object o1, Object o2)
    {
        if (o1 == null || o2 == null)
            return null;

        switch (widerType)
        {
        case DOUBLE:
            return ((Number) o1).doubleValue() >= ((Number) o2).doubleValue();
        case FLOAT:
            return ((Number) o1).floatValue() >= ((Number) o2).floatValue();
        case INT:
            return ((Number) o1).intValue() >= ((Number) o2).intValue();
        case LONG:
            return ((Number) o1).longValue() >= ((Number) o2).longValue();
        case STRING:
            return ((String) o1).compareTo(((String) o2)) >= 0;
        default:
            break;
        }
        return null;
    }

    @Override
    public ColumnType outputSchema(BlockSchema inputSchema) throws PreconditionException
    {
        switch (type)
        {
        case NOT:
            if (inputSchema.getColumnType(0).getType() != DataType.BOOLEAN)
                throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA,
                                                "NOT function should be applied to boolean value");
            break;
        case IS_NULL:
        case IS_NOT_NULL:
            break;
        case IN:
            // if (inputSchema.getColumnType(0).getType() != DataType.STRING)
            // throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA,
            // "IN function should be applied to string value");
            break;
        case EQ:
        case NE:
            if (!inputSchema.getColumnType(0).getType().isPrimitive())
                throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA,
                                                "LHS of "
                                                        + type
                                                        + " function is not a primitive value");
            if (!inputSchema.getColumnType(1).getType().isPrimitive())
                throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA,
                                                "RHS of "
                                                        + type
                                                        + " function is not a primitive value");
            widerType =
                    DataType.getWiderType(inputSchema.getColumnType(0).getType(),
                                          inputSchema.getColumnType(1).getType());
            break;
        case AND:
        case OR:
            if (inputSchema.getColumnType(0).getType() != DataType.BOOLEAN)
                throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA,
                                                "LHS of " + type
                                                        + " function is not boolean");
            if (inputSchema.getColumnType(1).getType() != DataType.BOOLEAN)
                throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA,
                                                "RHS of " + type
                                                        + " function is not boolean");
            break;
        case LT:
        case LE:
        case GT:
        case GE:
        {
            DataType leftType = inputSchema.getColumnType(0).getType();
            DataType rightType = inputSchema.getColumnType(1).getType();

            if (!leftType.isNumerical() && (leftType != DataType.STRING))
                throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA,
                                                "LHS of "
                                                        + type
                                                        + " function is not a numerical or string value");
            if (!rightType.isNumerical() && (rightType != DataType.STRING))
                throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA,
                                                "RHS of "
                                                        + type
                                                        + " function is not a numerical or value");

            if ((leftType.isNumerical() && !rightType.isNumerical())
                    || (!leftType.isNumerical() && rightType.isNumerical()))
            {
                throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA,
                                                "Incompatible data type for comparison: "
                                                        + leftType + " " + rightType);
            }

            widerType =
                    DataType.getWiderType(inputSchema.getColumnType(0).getType(),
                                          inputSchema.getColumnType(1).getType());

            if (widerType == null)
                widerType = DataType.STRING;

            break;
        }
        default:
            break;
        }

        return new ColumnType(null, DataType.BOOLEAN);
    }
}
