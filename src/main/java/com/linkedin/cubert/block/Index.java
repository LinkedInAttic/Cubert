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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.data.Tuple;

import com.linkedin.cubert.io.BlockSerializationType;
import com.linkedin.cubert.io.rubix.RubixConstants;
import com.linkedin.cubert.io.rubix.RubixFile;
import com.linkedin.cubert.io.rubix.RubixFile.KeyData;

/**
 * Represents the index for relation stored in rubix format.
 * 
 * @author Maneesh Varshney
 * 
 */
public class Index implements Serializable
{
    private static final long serialVersionUID = -7149581368638305803L;
    private final Map<Integer, List<IndexEntry>> entryMap =
            new HashMap<Integer, List<IndexEntry>>();
    private int numHashPartitions;
    private BlockSerializationType serializationType;

    private transient IndexEntry cachedEntry;
    private transient Map<Long, IndexEntry> blockIdMap;

    public static Index extractFromRelation(Configuration conf, String rootdir) throws IOException,
            InstantiationException,
            IllegalAccessException,
            ClassNotFoundException
    {
        Index index = new Index();

        Path globPath =
                new Path(new Path(rootdir), RubixConstants.RUBIX_EXTENSION_FOR_GLOB);
        FileStatus[] allFiles = FileSystem.get(conf).globStatus(globPath);

        for (FileStatus status : allFiles)
        {
            Path path = status.getPath();
            RubixFile<Tuple, Void> rubixFile = new RubixFile<Tuple, Void>(conf, path);

            index.serializationType = rubixFile.getBlockSerializationType();

            final List<KeyData<Tuple>> keyDataList = rubixFile.getKeyData();

            if (keyDataList == null || keyDataList.size() == 0)
            {
                // create an index entry with length = 0. how do we get
                // the hash partitionId?
                continue;
            }
            for (KeyData<Tuple> keyData : keyDataList)
            {
                final int reducerId = keyData.getReducerId();

                List<IndexEntry> entries = index.entryMap.get(reducerId);
                if (entries == null)
                {
                    entries = new ArrayList<IndexEntry>();
                    index.entryMap.put(reducerId, entries);
                }

                entries.add(new IndexEntry(path.toString(),
                                           keyData.getKey(),
                                           keyData.getOffset(),
                                           keyData.getLength(),
                                           keyData.getBlockId(),
                                           keyData.getNumRecords()));
            }
        }

        index.numHashPartitions = index.entryMap.size();

        for (Integer i : index.entryMap.keySet())
            Collections.sort(index.entryMap.get(i));

        return index;
    }

    public BlockSerializationType getSerializationType()
    {
        return serializationType;
    }

    public long getBlockId(Tuple key)
    {
        final int reducerId = getReducerId(key);
        List<IndexEntry> list = entryMap.get(reducerId);

        if (list == null)
            throw new RuntimeException("Cannot find blockid for " + key);

        if (cachedEntry == null)
        {
            cachedEntry = new IndexEntry(null, null, 0, 0, -1, -1);
        }

        cachedEntry.setKey(key);

        int idx = Collections.binarySearch(list, cachedEntry);

        if (idx >= 0)
        {
            return list.get(idx).getBlockId();
        }
        else
        {
            // if idx is negative, it refers to (-insertion point - 1)
            int insertPoint = -idx - 2;
            if (insertPoint == -1)
                insertPoint = 0;
            return list.get(insertPoint).getBlockId();
        }
    }

    public IndexEntry getEntry(long blockId)
    {
        if (blockIdMap == null)
            buildBlockIdMap();

        return blockIdMap.get(blockId);
    }

    public Set<Long> getAllBlockIds()
    {
        if (blockIdMap == null)
            buildBlockIdMap();
        return blockIdMap.keySet();
    }

    public IndexEntry getNextEntry(long blockId)
    {
        for (List<IndexEntry> list : entryMap.values())
        {
            for (int i = 0; i < list.size(); i++)
            {
                IndexEntry entry = list.get(i);
                if (blockId == entry.getBlockId())
                {
                    return i + 1 >= list.size() ? null : list.get(i + 1);
                }
            }
        }
        throw new RuntimeException("Couldn't locate block ID in Index entries.");
    }

    public int getReducerId(Tuple key)
    {
        int hashcode = BlockUtils.getBlockId(key);
        return hashcode % numHashPartitions;
    }

    private void buildBlockIdMap()
    {
        blockIdMap = new HashMap<Long, IndexEntry>();

        for (List<IndexEntry> list : entryMap.values())
        {
            for (IndexEntry entry : list)
            {
                blockIdMap.put(entry.getBlockId(), entry);
            }
        }
    }

    @Override
    public String toString()
    {
        int sum = 0;
        for (List<IndexEntry> entries : entryMap.values())
        {
            sum += entries.size();
        }

        return String.format("Index [entries=%d, numHashPartitions=%s, mapEntries:%s]",
                             sum,
                             numHashPartitions,
                             entryMap.toString());
    }

    public void print()
    {
        int sum = 0;
        for (List<IndexEntry> entries : entryMap.values())
        {
            sum += entries.size();
        }

        System.out.format("Index [entries=%d, numHashPartitions=%s]\n",
                          sum,
                          numHashPartitions);
        for (Integer key : entryMap.keySet())
        {
            System.out.println("Key: " + key);
            for (IndexEntry e : entryMap.get(key))
            {
                System.out.println("\t" + e);
            }
        }
    }

}
