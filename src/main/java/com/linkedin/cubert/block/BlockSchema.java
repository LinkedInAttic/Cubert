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

package com.linkedin.cubert.block;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

/**
 * Describes the schema of a block.
 * 
 * The description is a list of ColumnType for the fields in the Tuple.
 * 
 * @author Maneesh Varshney
 * 
 */
public class BlockSchema
{
    private final ColumnType[] columnTypes;
    private Map<String, Integer> indexMap;

    public BlockSchema(BlockSchema schema)
    {
        this.columnTypes = schema.columnTypes;
    }

    public BlockSchema(ColumnType[] columnTypes)
    {
        if (columnTypes == null)
            throw new IllegalArgumentException("input argument is null");

        this.columnTypes = columnTypes;
    }

    public BlockSchema(JsonNode json)
    {
        int ncols = json.size();
        columnTypes = new ColumnType[ncols];
        for (int i = 0; i < ncols; i++)
            columnTypes[i] = new ColumnType(json.get(i));
    }

    public BlockSchema(String str)
    {
        if (str == null)
            throw new IllegalArgumentException("input argument is null");

        String pairs[] = str.split(",");
        columnTypes = new ColumnType[pairs.length];

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
    }

    public int getNumColumns()
    {
        return columnTypes.length;
    }

    public String getName(int index)
    {
        return columnTypes[index].getName();
    }

    public DataType getType(int index)
    {
        return columnTypes[index].getType();
    }

    public ColumnType getColumnType(int index)
    {
        return columnTypes[index];
    }

    public ColumnType[] getColumnTypes()
    {
        return columnTypes;
    }

    public boolean hasIndex(String colName)
    {
        return getIndexMap().get(colName) != null;
    }

    public int getIndex(String columnName)
    {
        Integer index = getIndexMap().get(columnName);
        if (index == null)
            throw new IllegalArgumentException("Column [" + columnName
                    + "] is not part of schema : " + toString());

        return index;
    }

    public boolean isFlatSchema()
    {
        for (ColumnType ct : columnTypes)
            if (!ct.getType().isPrimitive())
                return false;
        return true;
    }

    public int getMemorySize()
    {
        int size = 0;
        for (ColumnType type : columnTypes)
        {
            switch (type.getType())
            {

            case BYTE:
                size += 1;
                break;
            case DOUBLE:
                size += 8;
                break;
            case FLOAT:
                size += 4;
                break;
            case INT:
                size += 4;
                break;
            case LONG:
                size += 8;
                break;
            default:
                throw new IllegalArgumentException("Cannot estimate memory size of Tuple with non-numerical fields");
            }

        }

        return size;
    }

    public Map<String, Integer> getIndexMap()
    {
        if (indexMap == null)
        {
            indexMap = new HashMap<String, Integer>();
            int idx = 0;
            for (ColumnType type : columnTypes)
            {
                indexMap.put(type.getName(), idx++);
            }
        }

        return indexMap;
    }

    /**
     * Returns an array of column names for the specified ColumnType array.
     * 
     * @return
     */
    public String[] getColumnNames()
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
     * Returns the subset of the input ColumnType array, where the name of the columns in
     * the subset matches the specified array of string.
     * 
     * The order of the ColumnType in the subset will be identical to the order specified
     * in the array of strings.
     * 
     * @param subset
     * @return
     */
    public BlockSchema getSubset(String[] subset)
    {
        ColumnType[] subsetTypes = new ColumnType[subset.length];
        Map<String, ColumnType> map = asMap();
        int idx = 0;
        for (String colname : subset)
        {
            ColumnType type = map.get(colname);
            if (type == null)
                throw new IllegalArgumentException("Column " + colname
                        + " is not present in ColumnTypes");

            subsetTypes[idx++] = type;
        }

        return new BlockSchema(subsetTypes);
    }

    /**
     * Returns the subset of the input ColumnType array where the name of columns in the
     * subset DO NOT match the specified array of string.
     * 
     * The order of ColumnType in the subset will be same as the order found in the input
     * ColumnType array.
     * 
     * @param subset
     * @return
     */
    public BlockSchema getComplementSubset(String[] subset)
    {
        ColumnType[] complement = new ColumnType[getNumColumns() - subset.length];
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

        return new BlockSchema(complement);
    }

    public BlockSchema append(BlockSchema other)
    {
        ColumnType[] first = this.columnTypes;
        ColumnType[] second = other.columnTypes;

        ColumnType[] append = new ColumnType[first.length + second.length];
        System.arraycopy(first, 0, append, 0, first.length);
        System.arraycopy(second, 0, append, first.length, second.length);

        return new BlockSchema(append);
    }

    public boolean allFieldsAllowShallowCopy()
    {
        for (ColumnType columnType : columnTypes)
            if (!columnType.getType().allowShallowCopy())
                return false;
        return true;
    }

    /**
     * Returns a map of column name and the column type for the specified ColumnType
     * array.
     * 
     * @return
     */
    public Map<String, ColumnType> asMap()
    {
        Map<String, ColumnType> map = new HashMap<String, ColumnType>();

        for (ColumnType type : columnTypes)
        {
            map.put(type.getName(), type);
        }

        return map;
    }

    public JsonNode toJson()
    {
        ArrayNode node = new ObjectMapper().createArrayNode();
        for (ColumnType ct : columnTypes)
            node.add(ct.toJson());

        return node;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BlockSchema other = (BlockSchema) obj;
        if (!Arrays.equals(columnTypes, other.columnTypes))
            return false;
        return true;
    }

    /**
     * Compares schemas by ignoring mismatches in the numerical types.
     *
     * @param other
     * @return
     */
    public boolean equalsIgnoreNumeric(BlockSchema other)
    {
        if (this.columnTypes.length != other.columnTypes.length)
            return false;

        for (int i = 0; i < columnTypes.length; i++)
        {
            ColumnType type1 = columnTypes[i];
            ColumnType type2 = other.columnTypes[i];

            if (!type1.getName().equals(type2.getName()))
                return false;

            if (type1.getType().isNumerical() && type2.getType().isNumerical())
                continue;

            if (!type1.matches(type2))
                return false;
        }

        return true;
    }

    @Override
    public String toString()
    {
        return Arrays.toString(columnTypes);
    }

}
