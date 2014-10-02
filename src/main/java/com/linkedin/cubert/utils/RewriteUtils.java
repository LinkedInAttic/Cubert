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

package com.linkedin.cubert.utils;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

public class RewriteUtils
{
    public static ObjectNode createProjectionExpressionNode(String outColName,
                                                            String inColName)
    {
        return JsonUtils.createObjectNode("col_name",
                                          outColName,
                                          "expression",
                                          createProjection(inColName));
    }

    public static ObjectNode createProjection(String gcol)
    {
        if (gcol.split("\\.").length == 1)
            return createSimpleColumnNode(gcol);
        else
            return createdNestedColumnNode(gcol);
    }

    private static ObjectNode createSimpleColumnNode(String gcol)
    {
        return createFunctionNode("PROJECTION",
                                  createTupleInputNode(),
                                  createStringConstant(gcol));
    }

    private static ObjectNode createdNestedColumnNode(String gcol)
    {

        String[] nestedFields = gcol.split("\\.");
        if (nestedFields.length < 2)
            throw new RuntimeException("Too few arguments in nested column expression for column "
                    + gcol + " split array len =" + nestedFields.length);
        ObjectNode toplevelColumn = createSimpleColumnNode(nestedFields[0]);

        ObjectNode childNode = toplevelColumn;
        ObjectNode resultNode = toplevelColumn;
        for (int i = 1; i < nestedFields.length; i++)
        {

            resultNode = JsonUtils.createObjectNode();
            resultNode.put("function", "PROJECTION");
            ArrayNode argsNode = JsonUtils.createArrayNode();
            argsNode.add(childNode);
            argsNode.add(createStringConstant(nestedFields[i]));
            resultNode.put("arguments", argsNode);
            childNode = resultNode;

        }
        return resultNode;
    }

    public static ObjectNode createTupleInputNode()
    {
        return JsonUtils.createObjectNode("function", "TUPLE_INPUT");
    }

    public static ObjectNode createStringConstant(String value)
    {
        return createFunctionNode("CONSTANT", value);
    }

    public static ObjectNode createIntegerConstant(Integer value)
    {
        return createFunctionNode("CONSTANT",
                                  JsonNodeFactory.instance.numberNode(value.intValue()));
    }

    public static JsonNode createFunctionExpressionNode(String outColName,
                                                        String funcName,
                                                        JsonNode... args)
    {
        return JsonUtils.createObjectNode("col_name",
                                          outColName,
                                          "expression",
                                          createFunctionNode(funcName, args));
    }

    public static JsonNode createFunctionExpressionNode(String outColName,
                                                        String funcName,
                                                        String... args)
    {
        return JsonUtils.createObjectNode("col_name",
                                          outColName,
                                          "expression",
                                          createFunctionNode(funcName, args));
    }

    public static ObjectNode createFunctionNode(String funcName, JsonNode... args)
    {
        ArrayNode argsNode = JsonUtils.createArrayNode(args);
        return JsonUtils.createObjectNode("function", funcName, "arguments", argsNode);
    }

    public static ObjectNode createFunctionNode(String funcName, String... args)
    {
        ArrayNode argsNode = JsonUtils.createArrayNode(args);
        return JsonUtils.createObjectNode("function", funcName, "arguments", argsNode);
    }

    public static String[] getInputRelations(ObjectNode opNode)
    {
        String[] inputRelations;
        if (opNode.get("input") instanceof ArrayNode)
            inputRelations = JsonUtils.asArray(opNode.get("input"));
        else
            inputRelations = new String[] { opNode.get("input").getTextValue() };
        return inputRelations;
    }

    public static boolean hasSummaryRewrite(ObjectNode programNode)
    {
        if (programNode.get("summaryRewrite") != null
                && programNode.get("summaryRewrite").getTextValue().equals("true"))
            return true;
        return false;
    }

}
