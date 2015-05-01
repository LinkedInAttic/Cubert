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
import java.util.ArrayList;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.BlockUtils;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.Pair;

/**
 * Partitions the data during the shuffle stage.
 * 
 * @author Maneesh Varshney
 * 
 */
public class CubertPartitioner<V> extends Partitioner<Tuple, V> implements Configurable
{
    private Tuple keyTuple;
    private ArrayList<Pair<Boolean, Integer>> partitionKeyIndex =
            new ArrayList<Pair<Boolean, Integer>>();

    @Override
    public void setConf(Configuration conf)
    {
        String jsonStr = conf.get(CubertStrings.JSON_SHUFFLE);
        try
        {
            JsonNode json = new ObjectMapper().readValue(jsonStr, JsonNode.class);
            String[] pivotKeys = JsonUtils.asArray(json.get("pivotKeys"));
            BlockSchema fullSchema = new BlockSchema(json.get("schema"));
            BlockSchema keySchema = fullSchema.getSubset(pivotKeys);
            BlockSchema valueSchema = fullSchema.getComplementSubset(pivotKeys);

            String[] partitionKeys = JsonUtils.asArray(json.get("partitionKeys"));

            keyTuple = TupleFactory.getInstance().newTuple(partitionKeys.length);

            for (int i = 0; i < partitionKeys.length; i++)
            {
                if (keySchema.hasIndex(partitionKeys[i]))
                {

                    partitionKeyIndex.add(new Pair<Boolean, Integer>(true,
                                                                     keySchema.getIndex(partitionKeys[i])));
                }
                else
                {
                    partitionKeyIndex.add(new Pair<Boolean, Integer>(false,
                                                                     valueSchema.getIndex(partitionKeys[i])));
                }
            }
            // print.l(partitionKeyIndex);
            // print.f("%s is the full schema", fullSchema.toString());
        }
        catch (JsonParseException e)
        {
            throw new RuntimeException(e);
        }
        catch (JsonMappingException e)
        {
            throw new RuntimeException(e);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Configuration getConf()
    {
        return null;
    }

    @Override
    public int getPartition(Tuple key, V value, int numPartitions)
    {
        try
        {
            for (int i = 0; i < partitionKeyIndex.size(); i++)
            {
                Object o = null;
                if (partitionKeyIndex.get(i).getFirst())
                    o = key.get(partitionKeyIndex.get(i).getSecond());
                else
                    o = ((Tuple) value).get(partitionKeyIndex.get(i).getSecond());

                keyTuple.set(i, o);
            }
        }
        catch (ExecException e)
        {
            e.printStackTrace();
        }

        // This has to be cast to long;
        // If it is int, and the hashcode is -2147483648, negative or math.abs(), or *-1
        // of this all return negative numbers
        long hashcode = BlockUtils.getBlockId(keyTuple);

        return (int) (hashcode % numPartitions);
    }
}
