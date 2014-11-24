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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.TupleStoreBlock;
import com.linkedin.cubert.utils.ClassCache;
import com.linkedin.cubert.utils.DefaultSortAlgo;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.RawTupleStore;
import com.linkedin.cubert.utils.SerializedTupleStore;
import com.linkedin.cubert.utils.SortAlgo;
import com.linkedin.cubert.utils.TupleStore;

/***
 * 
 * An operator that sorts the given block on a given set of keys and returns the sorted
 * data.
 * 
 * @author Krishna Puttaswamy
 * 
 */
public class SortOperator implements TupleOperator
{
    private Block block;
    private Iterator<Tuple> iterator;

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        block = input.values().iterator().next();

        String[] sortByColumns = JsonUtils.asArray(json, "sortBy");
        TupleStore store = getTupleStore(sortByColumns, props);
        Configuration conf = PhaseContext.getConf();
        String sortAlgoClass = conf.get("use.sort.algo");
        SortAlgo<Tuple> sa = null;

        if (sortAlgoClass != null)
        {
            try
            {
                sa =
                        (SortAlgo<Tuple>) ClassCache.forName(sortAlgoClass)
                                                    .asSubclass(SortAlgo.class)
                                                    .newInstance();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
        else
        {
            sa = new DefaultSortAlgo();
        }

        /* Sort the store */
        store.sort(sa);
        iterator = store.iterator();
    }

    public TupleStore getTupleStore(String[] sortByColumns, BlockProperties props) throws IOException,
            InterruptedException
    {
        if (block instanceof TupleStoreBlock)
        {
            TupleStore store = ((TupleStoreBlock) block).getStore();
            if (store instanceof RawTupleStore)
            {
                RawTupleStore rawStore = (RawTupleStore) store;
                rawStore.setSortKeys(sortByColumns);
                return rawStore;
            }
        }

        TupleStore store = new SerializedTupleStore(props.getSchema(), sortByColumns);

        Tuple tuple;
        while ((tuple = block.next()) != null)
        {
            store.addToStore(tuple);
        }
        return store;
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        if (iterator.hasNext())
            return iterator.next();
        else
            return null;
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        PostCondition condition = preConditions.values().iterator().next();
        BlockSchema inputSchema = condition.getSchema();
        String[] partitionKeys = condition.getPartitionKeys();
        String[] sortKeys = JsonUtils.asArray(json, "sortBy");

        return new PostCondition(inputSchema, partitionKeys, sortKeys);
    }
}
