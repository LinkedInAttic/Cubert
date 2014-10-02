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
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import com.linkedin.cubert.plan.physical.CubertStrings;

/**
 * A generic InputFormat that transparently handles multi mappers as well as combined file
 * splits.
 * 
 * @author Maneesh Varshney
 * 
 * @param <K>
 * @param <V>
 */
public class CubertInputFormat<K, V> extends InputFormat<K, V>
{

    private InputFormat<K, V> getActualInputFormat(JobContext context)
    {
        try
        {
            InputFormat<K, V> actualInputFormat =
                    (InputFormat<K, V>) context.getInputFormatClass().newInstance();
            if (actualInputFormat instanceof CubertInputFormat)
                throw new RuntimeException("No actual input format specified");

            return actualInputFormat;
        }
        catch (InstantiationException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IllegalAccessException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (ClassNotFoundException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException,
            InterruptedException
    {
        Configuration conf = context.getConfiguration();
        ConfigurationDiff confDiff = new ConfigurationDiff(conf);

        int numMultiMappers = confDiff.getNumDiffs();

        List<InputSplit> splits = new ArrayList<InputSplit>();

        for (int mapperIndex = 0; mapperIndex < numMultiMappers; mapperIndex++)
        {
            // reset conf to multimapper i
            confDiff.applyDiff(mapperIndex);

            // get the actual input format
            InputFormat<K, V> actualInputFormat = getActualInputFormat(context);

            List<InputSplit> actualSplits = null;

            // check if combined input split is requested
            boolean combineSplit = conf.getBoolean(CubertStrings.COMBINED_INPUT, false);

            if (combineSplit)
            {
                // Create CombinedFileInputFormat
                CombineFileInputFormat<K, V> cfif = new CombineFileInputFormat<K, V>()
                {
                    @Override
                    public RecordReader<K, V> createRecordReader(InputSplit split,
                                                                 TaskAttemptContext context) throws IOException
                    {
                        throw new IllegalStateException("Should not be called");
                    }
                };

                // get the splits
                actualSplits = cfif.getSplits(context);
            }
            else
            {
                actualSplits = actualInputFormat.getSplits(context);
            }

            // embed each split in MultiMapperSplit and add to list
            for (InputSplit actualSplit : actualSplits)
                splits.add(new MultiMapperSplit(actualSplit, mapperIndex));

            // undo the diff
            confDiff.undoDiff(mapperIndex);
        }
        return splits;
    }

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit split,
                                                 TaskAttemptContext context) throws IOException,
            InterruptedException
    {
        Configuration conf = context.getConfiguration();
        ConfigurationDiff confDiff = new ConfigurationDiff(conf);

        MultiMapperSplit mmSplit = (MultiMapperSplit) split;
        int multiMapperIndex = mmSplit.getMultiMapperIndex();

        confDiff.applyDiff(multiMapperIndex);

        // reset the conf to multiMapperIndex
        InputSplit actualSplit = mmSplit.getActualSplit();

        // get the actual input format class
        InputFormat<K, V> actualInputFormat = getActualInputFormat(context);

        RecordReader<K, V> reader = null;

        if (actualSplit instanceof CombineFileSplit)
        {
            reader =
                    new CombinedFileRecordReader<K, V>(actualInputFormat,
                                                       (CombineFileSplit) actualSplit,
                                                       context);
        }
        else
        {
            reader = actualInputFormat.createRecordReader(actualSplit, context);
        }

        // confDiff.undoDiff(multiMapperIndex);

        return new MultiMapperRecordReader<K, V>(reader);
    }

}
