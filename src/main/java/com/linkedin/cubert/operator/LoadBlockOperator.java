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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.BlockUtils;
import com.linkedin.cubert.block.EmptyBlock;
import com.linkedin.cubert.block.Index;
import com.linkedin.cubert.io.BlockSerializationType;
import com.linkedin.cubert.utils.FileCache;
import com.linkedin.cubert.utils.JsonUtils;

/***
 * A BlockOperator that create block by loading data directly from external files.
 * 
 * @author Maneesh Varshney
 * 
 */

public class LoadBlockOperator implements BlockOperator
{
    Block outputBlock;

    @Override
    public void setInput(Configuration conf, Map<String, Block> input, JsonNode json) throws IOException,
            InterruptedException
    {
        Block dataBlock = input.values().iterator().next();

        BlockSchema schema = new BlockSchema(json.get("schema"));
        long blockId = dataBlock.getProperties().getBlockId();

        BlockProperties props =
                new BlockProperties(JsonUtils.getText(json, "output"),
                                    schema,
                                    dataBlock.getProperties());

        if (blockId < 0)
        {
            outputBlock = new EmptyBlock(props);
        }
        else
        {
            try
            {
                Index index =
                        FileCache.get().getCachedIndex(JsonUtils.getText(json, "index"));
                boolean inMemory =
                        json.has("inMemory") && json.get("inMemory").getBooleanValue();
                BlockSerializationType serializationType = index.getSerializationType();

                outputBlock =
                        BlockUtils.loadBlock(props,
                                             index.getEntry(blockId),
                                             conf,
                                             json,
                                             serializationType,
                                             inMemory);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
        outputBlock.configure(json);
    }

    @Override
    public Block next() throws IOException,
            InterruptedException
    {
        Block ret = outputBlock;
        outputBlock = null;
        return ret;
    }
}
