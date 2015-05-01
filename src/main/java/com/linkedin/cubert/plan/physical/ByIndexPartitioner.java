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

package com.linkedin.cubert.plan.physical;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.Index;
import com.linkedin.cubert.utils.FileCache;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * Partitioner for BLOCKGEN BY INDEX. This partitioner expects a BLOCK_ID within the key
 * when shuffling. The partitioner extracts the reducerId of the BLOCK_ID (which are the
 * upper 32 bits) and use it to route it to the reducer.
 * <p>
 * This partitioner ensures that the skew of the partition keys in blockgen by index will
 * be identical to the skew in the original blockgen.
 *
 * @author Maneesh Varshney
 *
 * @param <V>
 */
public class ByIndexPartitioner<V> extends Partitioner<Tuple, V> implements Configurable
{
    private Configuration conf;
    private String indexName;
    private int blockIdIndex;
    private Map<Long, Integer> blockIdReducerMap;

    @Override
    public int getPartition(Tuple key, V value, int numPartitions)
    {
        if (blockIdReducerMap == null)
        {
            prepareBlockIdReducerMap();
        }

        long blockId;
        try
        {
            blockId = (Long) key.get(blockIdIndex);
        } catch (ExecException e)
        {
            throw new RuntimeException(e);
        }

        return blockIdReducerMap.get(blockId);
    }

    @Override
    public void setConf(Configuration conf)
    {
        this.conf = conf;
        try
        {
            final String jsonStr = conf.get(CubertStrings.JSON_SHUFFLE);
            final JsonNode json = new ObjectMapper().readValue(jsonStr, JsonNode.class);

            final String[] pivotKeys = JsonUtils.asArray(json.get("pivotKeys"));

            final BlockSchema fullSchema = new BlockSchema(json.get("schema"));
            final BlockSchema keySchema = fullSchema.getSubset(pivotKeys);

            blockIdIndex = keySchema.getIndex("BLOCK_ID");
            indexName = JsonUtils.getText(json, "index");
        }
        catch (JsonParseException e)
        {
            e.printStackTrace();
        }
        catch (JsonMappingException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private void prepareBlockIdReducerMap()
    {
        Index index;
        try
        {
            index = FileCache.getCachedIndex(indexName);
        } catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        int nReducers = conf.getInt("mapred.reduce.tasks", -1);
        if (nReducers == -1)
        {
            throw new RuntimeException("Unable to obtain number of reducers");
        }
        blockIdReducerMap = index.getBlockIdPartitionMap(nReducers);
        System.out.println("Successfully created BlockId versus reducer map. #blocks: "
                + blockIdReducerMap.size() + " #reducers: " + nReducers);
    }

    @Override
    public Configuration getConf()
    {
        return conf;
    }

}
