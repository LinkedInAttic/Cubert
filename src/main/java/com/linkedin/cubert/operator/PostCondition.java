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

package com.linkedin.cubert.operator;

import java.util.Arrays;

import com.linkedin.cubert.block.BlockSchema;

/**
 * Post condition of an operator.
 * 
 * The post condition comprises of: output block schema, list of partition keys and list
 * of sort keys.
 * 
 * @author Maneesh Varshney
 * 
 */
public class PostCondition
{
    private final BlockSchema schema;
    private final String[] partitionKeys;
    private final String[] sortKeys;
    private final String[] pivotKeys;

    public PostCondition(BlockSchema schema, String[] partitionKeys, String[] sortKeys)
    {
        this.schema = schema;
        this.partitionKeys = partitionKeys;
        this.sortKeys = sortKeys;
        this.pivotKeys = null;
    }

    public PostCondition(BlockSchema schema,
                         String[] partitionKeys,
                         String[] sortKeys,
                         String[] pivotKeys)
    {
        this.schema = schema;
        this.partitionKeys = partitionKeys;
        this.sortKeys = sortKeys;
        this.pivotKeys = pivotKeys;
    }

    public BlockSchema getSchema()
    {
        return schema;
    }

    public String[] getPartitionKeys()
    {
        return partitionKeys;
    }

    public String[] getSortKeys()
    {
        return sortKeys;
    }

    public String[] getPivotKeys()
    {
        return pivotKeys;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(schema)
               .append("  ")
               .append(Arrays.toString(partitionKeys))
               .append("  ")
               .append(Arrays.toString(sortKeys))
               .append("  ")
               .append(Arrays.toString(pivotKeys));
        return builder.toString();
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
        PostCondition other = (PostCondition) obj;
        if (!Arrays.equals(partitionKeys, other.partitionKeys))
            return false;
        if (schema == null)
        {
            if (other.schema != null)
                return false;
        }
        else if (!schema.equals(other.schema))
            return false;
        if (!Arrays.equals(sortKeys, other.sortKeys))
            return false;
        if (!Arrays.equals(pivotKeys, other.pivotKeys))
            return false;
        return true;
    }

}
