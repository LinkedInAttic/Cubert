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

import static com.linkedin.cubert.utils.JsonUtils.getText;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Defines the schema of a column.
 * 
 * The description includes the column name and data type.
 * 
 * @author Maneesh Varshney
 * 
 */
public class ColumnType
{
    private String name;

    private DataType type;

    private BlockSchema columnSchema;

    public ColumnType()
    {

    }

    public ColumnType(JsonNode json)
    {
        name = getText(json, "name");
        type = DataType.valueOf(getText(json, "type").toUpperCase());
        if (json.has("schema"))
            columnSchema = new BlockSchema(json.get("schema"));
    }

    public ColumnType(String name, DataType type)
    {
        this.name = name;
        this.type = type;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public DataType getType()
    {
        return type;
    }

    public void setType(DataType type)
    {
        this.type = type;
    }

    public void setType(String type)
    {
        this.type = DataType.valueOf(type.toUpperCase());
    }

    public BlockSchema getColumnSchema()
    {
        return columnSchema;
    }

    public void setColumnSchema(BlockSchema columnSchema)
    {
        this.columnSchema = columnSchema;
    }

    public JsonNode toJson()
    {
        ObjectNode node = new ObjectMapper().createObjectNode();
        node.put("name", name);
        node.put("type", type.toString());
        if (columnSchema != null)
            node.put("schema", columnSchema.toJson());
        return node;
    }

    @Override
    public String toString()
    {
        if (columnSchema == null)
            return String.format("%s %s", type, name);
        else
            return String.format("%s %s %s", type, name, columnSchema.toString());
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
        ColumnType other = (ColumnType) obj;
        return equals(other);
    }

    private boolean equals(ColumnType other)
    {
        if (name == null)
        {
            if (other.name != null)
            {
                return false;
            }
        }
        else if (!name.equals(other.name))
        {
            return false;
        }
        return matches(other);
    }

    public boolean matches(ColumnType other)
    {
        if (columnSchema == null)
        {
            if (other.columnSchema != null)
            {
                return false;
            }
        }
        else if (!columnSchema.equals(other.columnSchema))
        {
            return false;
        }

        return type == other.type;
    }

}
