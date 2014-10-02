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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.linkedin.cubert.plan.physical.CubertStrings;

public abstract class MultiMapperInputFormat<K, V> extends InputFormat<K, V>
{

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException,
            InterruptedException
    {
        Configuration conf = context.getConfiguration();
        int numMultiMappers = conf.getInt(CubertStrings.NUM_MULTI_MAPPERS, 1);
        List<InputSplit> splits = new ArrayList<InputSplit>();

        for (int i = 0; i < numMultiMappers; i++)
        {

            String dirs = conf.get(CubertStrings.MAPRED_INPUT_DIR + i);
            conf.set("mapred.input.dir", dirs);

            List<InputSplit> mapperSplits =
                    getDelegate(context.getConfiguration(), i).getSplits(context);

            for (InputSplit split : mapperSplits)
            {
                splits.add(new MultiMapperSplit((FileSplit) split, i));
            }
        }
        return splits;
    }

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit split,
                                                 TaskAttemptContext context) throws IOException,
            InterruptedException
    {
        MultiMapperSplit mmSplit = (MultiMapperSplit) split;
        int multiMapperIndex = mmSplit.getMultiMapperIndex();

        return getDelegate(context.getConfiguration(), multiMapperIndex).createRecordReader(mmSplit.getActualSplit(),
                                                                                            context);
    }

    protected abstract InputFormat<K, V> getDelegate(Configuration conf, int index);
}
