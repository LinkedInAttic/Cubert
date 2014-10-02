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

package com.linkedin.cubert.io;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A wrapper over a {@code RecordReader} that encapsulates the multi mapper index.
 * <p>
 * The primary responsibility of this wrapper class is to handle the {@code initialize}
 * method. The initialize method is given a {@code MultiMapperSplit} by hadoop, and this
 * class will extract the actual InputSplit out of the MultiMapperSplit and initialize the
 * actual RecordReader with this extracted InputSplit.
 * 
 * @author Maneesh Varshney
 * 
 * @param <KEYIN>
 * @param <VALUEIN>
 */
public class MultiMapperRecordReader<KEYIN, VALUEIN> extends RecordReader<KEYIN, VALUEIN>
{

    private final RecordReader<KEYIN, VALUEIN> delegate;

    public MultiMapperRecordReader(RecordReader<KEYIN, VALUEIN> delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException
    {
        delegate.initialize(((MultiMapperSplit) split).getActualSplit(), context);
    }

    @Override
    public boolean nextKeyValue() throws IOException,
            InterruptedException
    {
        return delegate.nextKeyValue();
    }

    @Override
    public KEYIN getCurrentKey() throws IOException,
            InterruptedException
    {
        return delegate.getCurrentKey();
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException,
            InterruptedException
    {
        return delegate.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException,
            InterruptedException
    {
        return delegate.getProgress();
    }

    @Override
    public void close() throws IOException
    {
        delegate.close();
    }

}
