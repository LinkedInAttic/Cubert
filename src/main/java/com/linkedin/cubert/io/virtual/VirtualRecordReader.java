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

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Record reader for the virtual storage.
 * The purpose of this data structure is to support a predecided number of mappers
 *
 * For the purposes of progress reporting. The nextKeyValue() returns true always! This implies that the map input
 * records counter will essentially report a garbage value. However, the number of bytes read is 0.
 *
 * @author Vinitha Gankidi
 * 
 * @param <K>
 * @param <V>
 */
public class VirtualRecordReader<K, V> extends RecordReader<K, V>
{
    private final FloatWritable progress = new FloatWritable(0);

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException
    {
    }

    @Override
    public boolean nextKeyValue() throws IOException,
            InterruptedException
    {
        return true;
    }

    @Override
    public K getCurrentKey() throws IOException,
            InterruptedException
    {
        return (K) progress;
    }

    @Override
    public V getCurrentValue() throws IOException,
            InterruptedException
    {
        return (V) progress;
    }

    @Override
    public float getProgress() throws IOException,
            InterruptedException
    {
        return progress.get();
    }

    @Override
    public void close() throws IOException
    {

    }

};
