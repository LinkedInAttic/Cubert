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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

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
    private static Path[] cachedFiles;
    private static Map<String, Index> cachedIndexMap = new HashMap<String, Index>(1);

    private static Configuration conf;

    public static void initialize(Configuration conf) throws IOException
    {
        FileCache.conf = conf;
        cachedFiles = DistributedCache.getLocalCacheFiles(conf);
    }

    public static String get(String path)
    {
        // first try to load the file using the symbolic link
        // (note that symbolic link does not work with hadoop 1 in local mode)
        try
        {
            URI uri = new URI(path);
            String fragment = uri.getFragment();
            if (fragment != null)
            {
                File file = new File(fragment);
                if (file.exists())
                    return file.toString();
            }

            // remove the fragment
            path = uri.getPath();
        }
        catch (URISyntaxException e)
        {
            // do nothing... fall through to the remaining code
        }

        // otherwise, try to load from full path

        if (cachedFiles == null)
            return null;

        for (Path cachedFile : cachedFiles)
        {
            if (cachedFile.toString().endsWith(path))
            {
                return cachedFile.toString();
            }
            if (cachedFile.getParent().toString().endsWith(path))
            {
                return cachedFile.toString();
            }
        }

        return null;
    }

    public static Index getCachedIndex(String indexName) throws IOException, ClassNotFoundException
    {
        Index index = cachedIndexMap.get(indexName);
        if (index != null)
        {
            return index;
        }

        final String prefix = conf.get(CubertStrings.JSON_CACHE_INDEX_PREFIX + indexName);
        final String indexFile = get(prefix + "#" + indexName);
        index = (Index) SerializerUtils.deserializeFromFile(indexFile);

        /* Cache the index */
        cachedIndexMap.put(indexName,  index);

        return index;
    }
}
