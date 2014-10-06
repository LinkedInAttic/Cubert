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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Generic record reader for {@code CombineFileSplit}.
 * <p>
 * This record reader extracts individual {@code FileSplit} from the
 * {@code CombineFileSplit}, and creates record readers for each of the FileSplits. The
 * record readers are created sequentially (a new one is created when the current record
 * reader has exhausted all data).
 * 
 * @author Maneesh Varshney
 * 
 * @param <K>
 * @param <V>
 */
public class CombinedFileRecordReader<K, V> extends RecordReader<K, V>
{
    private TaskAttemptContext context;
    private final InputFormat<K, V> inputFormat;
    private CombineFileSplit combineFileSplit;
    private RecordReader<K, V> current;
    private int currentFileIndex = 0;
    private float[] fractionLength;

    public CombinedFileRecordReader(InputFormat<K, V> inputFormat,
                                    CombineFileSplit combineFileSplit,
                                    TaskAttemptContext context)
    {
        this.inputFormat = inputFormat;
        this.combineFileSplit = combineFileSplit;
        this.context = context;

        long[] lengths = combineFileSplit.getLengths();
        long totalLength = 0;
        for (long length : lengths)
            totalLength += length;
        fractionLength = new float[lengths.length];
        for (int i = 0; i < lengths.length; i++)
            fractionLength[i] = ((float) lengths[i]) / totalLength;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException
    {
        createNewRecordReader();
    }

    /**
     * Create new record record from the original InputFormat and initialize it.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    private void createNewRecordReader() throws IOException,
            InterruptedException
    {
        FileSplit split =
                new FileSplit(combineFileSplit.getPath(currentFileIndex),
                              combineFileSplit.getOffset(currentFileIndex),
                              combineFileSplit.getLength(currentFileIndex),
                              null);

        if (split instanceof Configurable)
        {
            ((Configurable) split).setConf(context.getConfiguration());
        }

        current = inputFormat.createRecordReader(split, context);
        current.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException,
            InterruptedException
    {
        boolean next = current.nextKeyValue();
        if (next)
            return next;

        currentFileIndex++;

        // need to create new reader
        if (currentFileIndex == combineFileSplit.getNumPaths())
            return false;

        createNewRecordReader();
        return nextKeyValue();
    }

    @Override
    public K getCurrentKey() throws IOException,
            InterruptedException
    {
        return current.getCurrentKey();
    }

    @Override
    public V getCurrentValue() throws IOException,
            InterruptedException
    {
        return current.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException,
            InterruptedException
    {
        if (currentFileIndex >= combineFileSplit.getNumPaths())
            return (float) 1.0;

        float progress = 0;
        for (int i = 0; i < currentFileIndex - 1; i++)
            progress += fractionLength[i];
        progress += current.getProgress() * fractionLength[currentFileIndex];
        return progress;
    }

    @Override
    public void close() throws IOException
    {
        if (current != null)
            current.close();
        currentFileIndex = combineFileSplit.getPaths().length;
    }

}
