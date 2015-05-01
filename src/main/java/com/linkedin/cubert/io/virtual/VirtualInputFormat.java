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

package com.linkedin.cubert.io.virtual;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * The input format for the Virtual storage.
 * 
 * @author Vinitha Gankidi
 * 
 * @param <K>
 * @param <V>
 */
public class VirtualInputFormat<K, V> extends FileInputFormat<K, V>
{
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException
    {
        int numMappers = job.getConfiguration().getInt("mappers", -1);
        if (numMappers == -1)
            throw new RuntimeException("Number of mappers not set for virtual input format.");

        List<InputSplit> splits = new ArrayList<InputSplit>(numMappers);
        for (int i = 0; i < numMappers; i++)
            splits.add(new VirtualInputSplit<K, V>());
        return splits;
    }

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit split,
                                                 TaskAttemptContext context) throws IOException,
            InterruptedException
    {
        return new VirtualRecordReader<K, V>();
    }
}
