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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.cubert.block.ColumnType;

/**
 * Various utility methods for the column schema.
 * 
 * @author Maneesh Varshney
 * 
 */
public class ColumnTypeUtil
{
    private static ObjectMapper mapper;

    private static ObjectMapper getMapper()
    {
        if (mapper == null)
            mapper = new ObjectMapper();

        return mapper;
    }

    /**
     * Generates the ColumnType array for the specified string.
     * <p>
     * The specified string is of form "{@literal <data type> <column name>, ....}"
     * 
     * @param str
     *            the specified string
     * @return ColumnType array
     */
    public static ColumnType[] getColumnTypes(String str)
    {
        String pairs[] = str.split(",");
        ColumnType[] columnTypes = new ColumnType[pairs.length];

        int idx = 0;
        for (String pair : pairs)
        {
            pair = pair.trim();
            String[] typeName = pair.split("\\s+");
            String type = typeName[0].trim();
            String name = typeName[1].trim();
            ColumnType ctype = new ColumnType();
            ctype.setName(name);
            ctype.setType(type);
            columnTypes[idx++] = ctype;
        }

        return columnTypes;
    }

    /**
     * Generates the ColumnType array for the specified JsonNode.
     * <p>
     * The JsonNode can be:
     * <ul>
     * <li>a string of form "{@literal <data type> <column name>, ...}"</li>
     * 
     * <li>a json node that conforms to the ColumnType.json schema</li>
     * </ul>
     * 
     * @param node
     *            the json node
     * @return the ColumnType array
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    public static ColumnType[] getColumnTypes(JsonNode node) throws IOException
    {
        if (node.isTextual())
        {
            String str = node.getTextValue();
            return getColumnTypes(str);
        }
        else
        {
            return getMapper().readValue(node.toString(), ColumnType[].class);
        }
    }

    /**
     * Returns the subset of the input ColumnType array, where the name of the columns in
     * the subset matches the specified array of string.
     * <p>
     * The order of the ColumnType in the subset will be identical to the order specified
     * in the array of strings.
     * 
     * @param columnTypes
     * @param subset
     * @return
     */
    public static ColumnType[] getSubset(ColumnType[] columnTypes, String[] subset)
    {
        ColumnType[] subsetTypes = new ColumnType[subset.length];
        Map<String, ColumnType> map = asMap(columnTypes);
        int idx = 0;
        for (String colname : subset)
        {
            ColumnType type = map.get(colname);
            if (type == null)
                throw new IllegalArgumentException("Column " + colname
                        + " is not present in ColumnTypes");

            subsetTypes[idx++] = type;
        }

        return subsetTypes;
    }

    /**
     * Returns the subset of the input ColumnType array where the name of columns in the
     * subset DO NOT match the specified array of string.
     * <p>
     * The order of ColumnType in the subset will be same as the order found in the input
     * ColumnType array.
     * 
     * @param columnTypes
     * @param subset
     * @return
     */
    public static ColumnType[] getComplementSubset(ColumnType[] columnTypes,
                                                   String[] subset)
    {
        ColumnType[] complement = new ColumnType[columnTypes.length - subset.length];
        Set<String> set = new HashSet<String>();
        set.addAll(Arrays.asList(subset));

        int idx = 0;
        for (ColumnType type : columnTypes)
        {
            if (!set.contains(type.getName()))
            {
                complement[idx++] = type;
            }
        }

        return complement;
    }

    /**
     * Returns an array of column names for the specified ColumnType array.
     * 
     * @param columnTypes
     * @return
     */
    public static String[] getColumnNames(ColumnType[] columnTypes)
    {
        String[] colNames = new String[columnTypes.length];
        int ncols = colNames.length;
        for (int i = 0; i < ncols; i++)
        {
            colNames[i] = columnTypes[i].getName();
        }

        return colNames;
    }

    /**
     * Converts the ColumnType array into the Json string representation.
     * 
     * @param columnTypes
     * @return
     * @throws JsonGenerationException
     * @throws JsonMappingException
     * @throws IOException
     */
    public static String toString(ColumnType[] columnTypes) throws JsonGenerationException,
            JsonMappingException,
            IOException
    {
        return getMapper().writeValueAsString(columnTypes);
    }

    /**
     * Converts the specified ColumnType array as a JsonNode.
     * 
     * @param columnTypes
     * @return
     * @throws IOException
     */
    public static JsonNode toJson(ColumnType[] columnTypes) throws IOException
    {
        String str = getMapper().writeValueAsString(columnTypes);
        return getMapper().readValue(str, JsonNode.class);
    }

    /**
     * Returns a map of column name and the column type for the specified ColumnType
     * array.
     * 
     * @param columnTypes
     * @return
     */
    public static Map<String, ColumnType> asMap(ColumnType[] columnTypes)
    {
        Map<String, ColumnType> map = new HashMap<String, ColumnType>();

        for (ColumnType type : columnTypes)
        {
            map.put(type.getName(), type);
        }

        return map;
    }

}
