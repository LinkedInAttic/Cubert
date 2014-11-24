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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.LocalFileBlock;
import com.linkedin.cubert.utils.FileCache;

/**
 * A BlockOperator that create block by loading data directly from external files.
 * 
 * @author Maneesh Varshney
 * 
 */
public class LoadBlockFromCacheOperator implements BlockOperator
{
    private Block block;

    @Override
    public void setInput(Configuration conf, Map<String, Block> input, JsonNode json) throws IOException,
            InterruptedException
    {
        // FileCache fileCache = new FileCache(conf);

        String path = json.get("path").getTextValue();
        String cachedPath = FileCache.get(path);
        if (cachedPath == null)
            throw new IOException("Cannot find file in dist cache: " + path);

        block = new LocalFileBlock(new File(cachedPath));
        block.configure(json);
    }

    @Override
    public Block next() throws IOException,
            InterruptedException
    {
        Block retVal = block;
        block = null;
        return retVal;
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        // TODO Auto-generated method stub
        return null;
    }

}
