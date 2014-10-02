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

package com.linkedin.cubert.io.text;

import java.io.IOException;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextOutputFormat;
import org.apache.pig.data.Tuple;

import com.linkedin.cubert.plan.physical.CubertStrings;

/***
 * 
 * The wrapper around pig text output format. Provides the ability to set the appropriate
 * field separator while writing the text file. We need this wrapper because
 * PigTextOutputFormat doesn't provide empty constructor, which means it's objects cannot
 * be created via reflection. This wrapper solves both problems.
 * 
 * @author Krishna Puttaswamy
 * 
 * */

public class PigTextOutputFormatWrapper<K, V> extends PigTextOutputFormat
{
    public static byte defaultDelimiter = '\t';

    public PigTextOutputFormatWrapper()
    {
        super(defaultDelimiter);
    }

    public PigTextOutputFormatWrapper(byte delimiter)
    {
        super(delimiter);
    }

    @Override
    public RecordWriter<WritableComparable, Tuple> getRecordWriter(TaskAttemptContext context) throws IOException,
            InterruptedException
    {
        Configuration conf = context.getConfiguration();

        PigTextOutputFormat outputFormat;
        if (conf.get(CubertStrings.TEXT_OUTPUT_SEPARATOR) == null)
        {
            outputFormat = new PigTextOutputFormat(defaultDelimiter);
        }
        else
        {
            String str = conf.get(CubertStrings.TEXT_OUTPUT_SEPARATOR);
            str = StringEscapeUtils.unescapeJava(str);
            byte[] bytes = str.getBytes("UTF-8");

            if (bytes.length > 1)
                throw new RuntimeException(String.format("Invalid separator in text output format %s",
                                                         str));

            outputFormat = new PigTextOutputFormat(bytes[0]);
        }

        return outputFormat.getRecordWriter(context);
    }
}
