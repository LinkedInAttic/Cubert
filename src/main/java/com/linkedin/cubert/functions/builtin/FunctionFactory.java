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

import org.apache.pig.EvalFunc;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.functions.Function;
import com.linkedin.cubert.functions.PigEvalFuncWrapper;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.ClassCache;

/**
 * Creates a Function object. The function can be built-in function, a user-defined
 * function that subclasses {@code Function}, or a user-defined function that subclasses
 * Pig's {@code EvalFunc}.
 *
 * @author Maneesh Varshney
 *
 */
public class FunctionFactory
{
    @SuppressWarnings("rawtypes")
    public static Function get(String name, JsonNode constructorArgs)
    {
        // first see if this is built in function
        Function func = getBuiltinFunction(name);

        if (func != null)
            return func;

        // otherwise, this must be a UDF
        Object obj = createFunctionObject(name, constructorArgs);

        if (obj instanceof Function)
            return (Function) obj;

        if (obj instanceof EvalFunc)
            return new PigEvalFuncWrapper((EvalFunc) obj);

        throw new RuntimeException(name + " is not a valid builtin function or a UDF");
    }

    private static Function getBuiltinFunction(String name)
    {
        FunctionType type = getBuiltinFunctionType(name);
        if (type == null)
            return null;

        switch (type)
        {
        case ADD:
        case MINUS:
        case TIMES:
        case DIVIDE:
        case MOD:
        case LSHIFT:
            return new ArithmeticFunction(type);
        case NOT:
        case IS_NULL:
        case IS_NOT_NULL:
        case IN:
        case EQ:
        case NE:
        case AND:
        case OR:
        case LT:
        case LE:
        case GT:
        case GE:
            return new BooleanFunction(type);
        case MATCHES:
            return new Match();
        case NVL:
            return new Nvl();
        case CASTTOINT:
            return new Typecast(DataType.INT);
        case CASTTOLONG:
            return new Typecast(DataType.LONG);
        case CASTTOFLOAT:
            return new Typecast(DataType.FLOAT);
        case CASTTODOUBLE:
            return new Typecast(DataType.DOUBLE);
        case CASTTOSTRING:
            return new Typecast(DataType.STRING);
        case SIZEOF:
            return new Sizeof();
        case CASE:
            return new Case();
        case CONCAT:
            return new Concat();
        case UNIQUEID:
            return new UniqueId();
        case SEQNO:
            return new SeqNo();
        default:
            break;

        }

        return null;
    }

    private static FunctionType getBuiltinFunctionType(String name)
    {
        for (FunctionType ft : FunctionType.values())
        {
            if (ft.name().equalsIgnoreCase(name))
            {
                return ft;
            }
        }
        return null;
    }

    public static Object createFunctionObject(String name, JsonNode constructorArgs)
    {
        try
        {
            Class<?> cls = ClassCache.forName(name);
            if (constructorArgs == null || constructorArgs.isNull())
                return cls.newInstance();

            Object[] args = new Object[constructorArgs.size()];
            Class<?>[] argClasses = new Class[args.length];
            for (int i = 0; i < args.length; i++)
            {
                args[i] = JsonUtils.asObject(constructorArgs.get(i));
                argClasses[i] = args[i].getClass();
            }

            return cls.getConstructor(argClasses).newInstance(args);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

    }
}
