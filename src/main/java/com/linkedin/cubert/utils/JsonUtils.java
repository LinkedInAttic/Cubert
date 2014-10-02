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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser.NumberType;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Various utility methods for operating with Json objects.
 * 
 * @author Maneesh Varshney
 * 
 */
public class JsonUtils
{
    private static ObjectMapper mapper;

    private static ObjectMapper getMapper()
    {
        if (mapper == null)
        {
            mapper = new ObjectMapper();
        }

        return mapper;
    }

    public static JsonNode get(JsonNode node, String property)
    {
        JsonNode val = node.get(property);
        if (val == null)
        {
            throw new IllegalArgumentException("Property " + property
                    + " is not defined in " + node);
        }
        return val;
    }

    public static String getText(JsonNode node, String property, String defaultValue)
    {
        if (!node.has(property))
            return defaultValue;
        return get(node, property).getTextValue();
    }

    public static String getText(JsonNode node, String property)
    {
        return get(node, property).getTextValue();
    }

    public static String[] asArray(JsonNode parent, String property)
    {
        if (!parent.has(property) || parent.get(property).isNull())
            return null;

        return asArray(get(parent, property));
    }

    public static String[] asArray(JsonNode node)
    {
        if (node == null)
            throw new IllegalArgumentException("Specified JsonNode is null");

        if (node.isArray())
        {
            ArrayNode anode = (ArrayNode) node;
            int nelements = anode.size();
            String[] array = new String[nelements];
            for (int i = 0; i < nelements; i++)
            {
                array[i] = anode.get(i).getTextValue();
            }
            return array;
        }
        else
        {
            return new String[] { node.getTextValue() };
        }
    }

    public static Object asObject(JsonNode node)
    {
        if (node.isTextual())
            return node.getTextValue();
        else if (node.isInt())
            return node.getIntValue();
        else if (node.isFloatingPointNumber())
            return node.getDoubleValue();
        else if (node.isBoolean())
            return node.getBooleanValue();

        return null;
    }

    public static ObjectNode createObjectNode(Object... keyvals)
    {
        ObjectNode node = getMapper().createObjectNode();
        for (int i = 0; i < keyvals.length; i += 2)
        {
            if (keyvals[i + 1] instanceof String)
            {
                node.put((String) keyvals[i], (String) keyvals[i + 1]);
            }
            else if (keyvals[i + 1] instanceof Integer)
            {
                int val = (Integer) keyvals[i + 1];
                node.put((String) keyvals[i], val);
            }
            else if (keyvals[i + 1] instanceof Boolean)
            {
                boolean val = (Boolean) keyvals[i + 1];
                node.put((String) keyvals[i], val);
            }
            else
            {
                node.put((String) keyvals[i], (JsonNode) keyvals[i + 1]);
            }
        }

        return node;
    }

    public static ArrayNode createArrayNode(String item)
    {
        return createArrayNode(new String[] { item });
    }

    public static ArrayNode createArrayNode()
    {
        return getMapper().createArrayNode();
    }

    public static ArrayNode createArrayNode(JsonNode... elementNodes)
    {
        ArrayNode result = getMapper().createArrayNode();
        for (int i = 0; i < elementNodes.length; i++)
            result.add(elementNodes[i]);
        return result;
    }

    public static ArrayNode createArrayNode(String[] items)
    {
        ArrayNode anode = getMapper().createArrayNode();
        for (String item : items)
        {
            anode.add(item);
        }
        return anode;
    }

    public static int getIndexFromArray(ArrayNode anode, JsonNode element)
    {
        for (int i = 0; i < anode.size(); i++)
        {
            if (anode.get(i) == element)
                return i;
        }
        return -1;
    }

    public static void deleteFromArrayNode(ArrayNode parentNode, ObjectNode operatorNode)
    {
        // TODO Auto-generated method stub
        int index = getIndexFromArray((ArrayNode) parentNode, operatorNode);
        ((ArrayNode) parentNode).remove(index);
    }

    public static JsonNode makeJson(String str)
    {
        str = str.replace('\'', '"');
        try
        {
            return mapper.readValue(str, JsonNode.class);
        }
        catch (JsonParseException e)
        {
            int idx = (int) e.getLocation().getCharOffset();
            System.out.println(str);
            if (idx >= str.length())
            {
                idx = 1;
            }
            String substr = str.substring(0, idx);
            System.out.println(substr);
            e.printStackTrace();
        }
        catch (JsonMappingException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IOException e)
        {

        }
        throw new RuntimeException();
    }

    public static ArrayList<JsonNode> toArrayList(ArrayNode arrayNode)
    {
        ArrayList<JsonNode> result = new ArrayList<JsonNode>();
        for (JsonNode elem : arrayNode)
            result.add(elem);
        return result;
    }

    public static ArrayNode toArrayNode(List<JsonNode> nodelist)
    {
        ArrayNode result = createArrayNode();
        for (JsonNode elem : nodelist)
            result.add(elem);
        return result;
    }

    public static JsonNode cloneNode(JsonNode json)
    {
        String str = json.toString();
        try
        {
            return getMapper().readValue(str, JsonNode.class);
        }
        catch (JsonParseException e)
        {
            e.printStackTrace();
            return null;
        }
        catch (JsonMappingException e)
        {
            e.printStackTrace();
            return null;
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return null;
        }
    }

    public static String encodePath(JsonNode path)
    {
        if (path.isTextual())
            return path.getTextValue();
        else
        {
            String root = getText(path, "root");
            int startDate = Integer.parseInt(getText(path, "startDate"));
            int endDate = Integer.parseInt(getText(path, "endDate"));
            return String.format("%s#START%d#END%d", root, startDate, endDate);
        }
    }

    public static JsonNode decodePath(String pathStr)
    {
        ObjectNode node = getMapper().createObjectNode();

        if (pathStr.contains("#START"))
        {
            String root = pathStr.split("#START")[0];
            String[] startEnd = pathStr.split("#START")[1].split("#END");
            int startDate = Integer.parseInt(startEnd[0]);
            int endDate = Integer.parseInt(startEnd[1]);

            node.put("root", root);
            node.put("startDate", startDate);
            node.put("endDate", endDate);
            return node;
        }
        else
        {
            node.put("tmp", pathStr);
            return node.get("tmp");
        }
    }

    public static boolean isPrefixArray(ArrayNode pnode, ArrayNode snode)
    {
        if (pnode == null || snode == null || pnode.size() > snode.size())
            return false;

        for (int i = 0; i < pnode.size(); i++)
        {
            if (!pnode.get(i).getTextValue().equals(snode.get(i).getTextValue()))
                return false;
        }
        return true;
    }

    public static ArrayNode insertNodeListAfter(ArrayNode arrayNode,
                                                ObjectNode afterThisNode,
                                                List<ObjectNode> nodeList)
    {
        ArrayList<JsonNode> currentNodeList = JsonUtils.toArrayList(arrayNode);
        int insertPosition = CommonUtils.indexOfByRef(currentNodeList, afterThisNode);
        currentNodeList.addAll(insertPosition + 1, nodeList);
        return JsonUtils.toArrayNode(currentNodeList);
    }

    public static ArrayNode insertNodeListBefore(ArrayNode arrayNode,
                                                 ObjectNode beforeThisNode,
                                                 List<ObjectNode> nodeList)
    {
        ArrayList<JsonNode> currentNodeList = JsonUtils.toArrayList(arrayNode);
        int insertPosition = CommonUtils.indexOfByRef(currentNodeList, beforeThisNode);
        currentNodeList.addAll(insertPosition, nodeList);
        return JsonUtils.toArrayNode(currentNodeList);
    }

    public static Object decodeConstant(JsonNode valNode, String valType)
    {
        if (valType != null && valNode.isNumber())
        {
            NumberType numType;
            Number numVal = valNode.getNumberValue();

            if (valType != null)
            {
                numType = NumberType.valueOf(valType.toUpperCase());
                switch (numType)
                {
                case DOUBLE:
                    return numVal.doubleValue();
                case FLOAT:
                    return numVal.floatValue();
                case INT:
                    return numVal.intValue();
                case LONG:
                    return numVal.longValue();
                default:
                    break;

                }
            }
        }

        return asObject(valNode);
    }

    public static void prettyPrint(String json) throws JsonGenerationException,
            JsonMappingException,
            IOException
    {
        System.out.println(new ObjectMapper().writer().writeValueAsString(json));
    }

}
