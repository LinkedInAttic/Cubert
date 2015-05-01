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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextOutputFormat;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.io.TeeWriter;
import com.linkedin.cubert.operator.PhaseContext;
import com.linkedin.cubert.utils.JsonUtils;


/**
 * Writes TEE data in TEXT format.
 * 
 * @author Maneesh Varshney
 * 
 */
public class TextTeeWriter implements TeeWriter
{
    /**
     * The whole purpose of this delegate is to have access to the PigLineRecordWriter
     * class, which is a nested protected class.
     * 
     * @author Maneesh Varshney
     * 
     */
    private static final class Delegate extends PigTextOutputFormat
    {
        PigLineRecordWriter writer;

        public Delegate(DataOutputStream out, byte delimiter)
        {
            super(delimiter);
            writer = new PigLineRecordWriter(out, delimiter);

        }

        public void write(Tuple tuple) throws IOException
        {
            writer.write(null, tuple);
        }
     }

    private Delegate delegate;
    private DataOutputStream out;

    @Override
    public void open(Configuration conf,
                     JsonNode json,
                     BlockSchema schema,
                     Path root,
                     String filename) throws IOException
    {

        String separator = "\t";
        if (json.has("params") && !json.get("params").isNull() && json.get("params").has("separator"))
        {
            separator = JsonUtils.getText(json.get("params"), "separator");
        }

        separator = StringEscapeUtils.unescapeJava(separator);
        byte[] bytes = separator.getBytes("UTF-8");

        if (bytes.length > 1)
        {
            throw new RuntimeException(String.format("Invalid separator in text output format %s", separator));
        }

        final TaskAttemptContext context = PhaseContext.isMapper() ?
                PhaseContext.getMapContext() :
                PhaseContext.getRedContext();

        String extension = "";
        CompressionCodec codec = null;
        final boolean isCompressed = FileOutputFormat.getCompressOutput(context);
        if (isCompressed)
        {
            Class<? extends CompressionCodec> codecClass =
                    FileOutputFormat.getOutputCompressorClass(context, GzipCodec.class);

            codec = ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }

        out = FileSystem.get(conf).create(new Path(root, filename + extension));
        if (isCompressed)
        {
            out = new DataOutputStream(codec.createOutputStream(out));
        }

        delegate = new Delegate(out, bytes[0]);
    }

    @Override
    public void write(Tuple tuple) throws IOException
    {
        delegate.write(tuple);
    }

    @Override
    public void close() throws IOException
    {
        out.close();
    }

    @Override
    public void flush() throws IOException
    {
        out.flush();
    }
}
