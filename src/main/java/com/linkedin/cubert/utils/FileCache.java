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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

import com.linkedin.cubert.block.Index;
import com.linkedin.cubert.io.SerializerUtils;
import com.linkedin.cubert.plan.physical.CubertStrings;

/**
 * Utility method to access the DistributedCache.
 * 
 * @author Maneesh Varshney
 * 
 */
public class FileCache
{
    private final Path[] cachedFiles;
    private static FileCache fileCache;
    private Configuration conf;

    public FileCache(Configuration conf) throws IOException
    {
        this.conf = conf;
        cachedFiles = DistributedCache.getLocalCacheFiles(conf);
    }

    public static FileCache create(Configuration conf) throws IOException
    {
        fileCache = new FileCache(conf);
        return fileCache;
    }

    public static FileCache get()
    {
        if (fileCache == null)
        {
            throw new RuntimeException("FileCache has not been created yet.");
        }
        return fileCache;
    }

    public String getCachedFile(String suffix)
    {
        if (cachedFiles == null)
            return null;

        for (Path cachedFile : cachedFiles)
        {
            if (cachedFile.toString().endsWith(suffix))
            {
                return cachedFile.toString();
            }
            if (cachedFile.getParent().toString().endsWith(suffix))
            {
                return cachedFile.toString();
            }
        }

        return null;
    }

    public Index getCachedIndex(String indexName) throws FileNotFoundException,
            IOException,
            ClassNotFoundException
    {
        String suffix = conf.get(CubertStrings.JSON_CACHE_INDEX_PREFIX + indexName);
        String indexFile = getCachedFile(suffix);
        return (Index) SerializerUtils.deserializeFromFile(indexFile);
    }
}
