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

package com.linkedin.cubert.io.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.linkedin.cubert.pig.piggybank.storage.avro.PigAvroOutputFormat;

public class PigAvroOutputFormatAdaptor extends FileOutputFormat<NullWritable, Object>
{

    private PigAvroOutputFormat delegate;

    @Override
    public RecordWriter<NullWritable, Object> getRecordWriter(TaskAttemptContext context) throws IOException,
            InterruptedException
    {
        return getDelegate(context.getConfiguration()).getRecordWriter(context);
    }

    private PigAvroOutputFormat getDelegate(Configuration conf)
    {
        if (delegate == null)
        {
            String schemaStr = conf.get("cubert.avro.output.schema");
            Schema schema = new Schema.Parser().parse(schemaStr);
            delegate = new PigAvroOutputFormat(schema);
        }

        return delegate;
    }

}
