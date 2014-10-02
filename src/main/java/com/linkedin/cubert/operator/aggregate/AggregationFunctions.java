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

package com.linkedin.cubert.operator.aggregate;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import com.linkedin.cubert.operator.PhaseContext;
import com.linkedin.cubert.utils.JsonUtils;

public class AggregationFunctions
{
    public static AggregationFunction get(AggregationType type, JsonNode aggregateJson)
    {
        switch (type)
        {
        case COUNT:
            return new CountAggregation();
        case MAX:
            return new MaxAggregation();
        case MIN:
            return new MinAggregation();
        case SUM:
            return new SumAggregation();
        case USER_DEFINED_AGGREGATION:
            return createUDAFAggregationOperator(aggregateJson);
        case BAG:
        case CREATE_ARRAYLIST:
            return new ArrayListAggregation();
        case BITWISE_OR:
            return new BitwiseORAggregation();
        case COUNT_DISTINCT:
            return new CountDistinctAggregation();
        default:
            break;

        }

        return null;
    }

    public static boolean isUserDefinedAggregation(String aggFunction)
    {
        try
        {
            AggregationType.valueOf(aggFunction);
            return false;
        }
        catch (IllegalArgumentException e)
        {
            return true;
        }

    }

    public static AggregationFunction createUDAFAggregationOperator(JsonNode aggregateJson)
    {
        String className = aggregateJson.get("udaf").getTextValue();
        Object udafObj = PhaseContext.getUDF(className);
        if (udafObj != null)
            return ((AggregationFunction) udafObj);

        try
        {
            Class<?> udafClass = Class.forName(className);
            ArrayNode argsNode = null;
            if (aggregateJson.get("constructorArgs") != null)
                argsNode = (ArrayNode) aggregateJson.get("constructorArgs");
            AggregationFunction udafFunc = null;

            Object[] args = new Object[argsNode.size()];
            for (int i = 0; i < args.length; i++)
            {
                args[i] = JsonUtils.asObject(argsNode.get(i));
            }

            if (args == null || args.length == 0)
            {
                udafFunc = (AggregationFunction) udafClass.newInstance();
            }
            else
            {
                Class<?>[] argClasses = new Class[args.length];
                for (int i = 0; i < args.length; i++)
                    argClasses[i] = args[i].getClass();
                udafFunc =
                        (AggregationFunction) udafClass.getConstructor(argClasses)
                                                       .newInstance(args);
                PhaseContext.insertUDF(className, udafFunc);
            }

            return udafFunc;
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new RuntimeException("Failed to instantiate UDAF class due to " + e);
        }

    }
}
