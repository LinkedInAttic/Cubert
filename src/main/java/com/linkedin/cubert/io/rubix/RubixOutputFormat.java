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

package com.linkedin.cubert.io.rubix;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class RubixOutputFormat<K, V> extends FileOutputFormat<K, V>
{
    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException,
            InterruptedException
    {
        Configuration conf = context.getConfiguration();
        String extension = RubixConstants.RUBIX_EXTENSION_FOR_WRITE;

        CompressionCodec codec = null;
        boolean isCompressed = getCompressOutput(context);

        if (isCompressed)
        {
            Class<?> codecClass = getOutputCompressorClass(context, DefaultCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            extension += codec.getDefaultExtension();
        }

        Path file = getDefaultWorkFile(context, extension);
        FileSystem fs = file.getFileSystem(conf);

        FSDataOutputStream fileOut = fs.create(file, false);
        return new RubixRecordWriter<K, V>(conf,
                                           fileOut,
                                           context.getOutputKeyClass(),
                                           context.getOutputValueClass(),
                                           codec);
    }
}
