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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.data.Tuple;

import com.linkedin.cubert.utils.Pair;

/**
 * Stores the meta data for a block.
 * 
 * @author Maneesh Varshney
 * 
 */
public final class BlockProperties
{
    private enum StandardKeys
    {
        BLOCK_ID_KEY, NUM_RECORDS_KEY, PARTITION_KEY
    }

    private final String blockName;
    private final BlockSchema schema;
    private final BlockProperties[] parents;

    private final Map<String, Object> properties = new HashMap<String, Object>();

    public BlockProperties(String blockName, BlockSchema schema, BlockProperties parent)
    {
        this(blockName, schema, parent == null ? null : new BlockProperties[] { parent });
    }

    public BlockProperties(String blockName, BlockSchema schema, BlockProperties[] parents)
    {
        this.blockName = blockName;
        this.schema = schema;
        this.parents = parents;
    }

    public BlockSchema getSchema()
    {
        return schema;
    }

    public Long getBlockId()
    {
        return (Long) get(StandardKeys.BLOCK_ID_KEY.toString());
    }

    public Long getNumRecords()
    {
        return (Long) get(StandardKeys.NUM_RECORDS_KEY.toString());
    }

    public Tuple getPartitionKey()
    {
        return (Tuple) get(StandardKeys.PARTITION_KEY.toString());
    }

    public void setNumRecords(long numRecords)
    {
        properties.put(StandardKeys.NUM_RECORDS_KEY.toString(), numRecords);
    }

    public void setBlockId(long blockId)
    {
        properties.put(StandardKeys.BLOCK_ID_KEY.toString(), blockId);
    }

    public void setPartitionKey(Tuple partitionKey)
    {
        properties.put(StandardKeys.PARTITION_KEY.toString(), partitionKey);
    }

    private Object get(String key)
    {
        Object val = properties.get(key);
        if (val != null)
            return val;

        if (parents == null)
            return null;

        for (BlockProperties parent : parents)
        {
            val = parent.get(key);
            if (val != null)
                return val;
        }

        return null;
    }

    public List<Pair<String, Object>> getAll(String key)
    {
        List<Pair<String, Object>> list = new ArrayList<Pair<String, Object>>();
        getAll(key, list);
        return list;
    }

    private void getAll(String key, List<Pair<String, Object>> list)
    {
        Object val = properties.get(key);
        if (val != null)
            list.add(new Pair<String, Object>(blockName, val));
        if (parents == null)
            return;

        for (BlockProperties parent : parents)
            parent.getAll(key, list);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("BlockProperties [blockName=")
               .append(blockName)
               .append(", schema=")
               .append(schema)
               .append(", properties=")
               .append(properties)
               .append("]");
        return builder.toString();
    }

}
