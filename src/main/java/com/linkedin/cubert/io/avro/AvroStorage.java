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
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.BlockWriter;
import com.linkedin.cubert.block.TupleCreator;
import com.linkedin.cubert.io.CachedFileReader;
import com.linkedin.cubert.io.Storage;
import com.linkedin.cubert.io.TeeWriter;
import com.linkedin.cubert.operator.PostCondition;
import com.linkedin.cubert.utils.AvroUtils;

public class AvroStorage implements Storage
{

    @Override
    public void prepareInput(Job job,
                             Configuration conf,
                             JsonNode params,
                             List<Path> paths) throws IOException
    {
        Schema avroSchema = AvroUtils.getSchema(conf, paths.get(0));
        // set the schema for this index
        conf.set("cubert.avro.input.schema", avroSchema.toString());
        if (params.has("unsplittable")
                && Boolean.parseBoolean(params.get("unsplittable").getTextValue()))
            conf.set("cubert.avro.input.unsplittable", "true");
        else
            conf.set("cubert.avro.input.unsplittable", "false");
        job.setInputFormatClass(PigAvroInputFormatAdaptor.class);
    }

    @Override
    public void prepareOutput(Job job,
                              Configuration conf,
                              JsonNode params,
                              BlockSchema schema,
                              Path path)
    {
        Schema avroSchema = AvroUtils.convertFromBlockSchema("record", schema);
        conf.set("cubert.avro.output.schema", avroSchema.toString());
        job.setOutputFormatClass(PigAvroOutputFormatAdaptor.class);
    }

    @Override
    public TupleCreator getTupleCreator()
    {
        return new AvroTupleCreator();
    }

    @Override
    public BlockWriter getBlockWriter()
    {
        return new AvroBlockWriter();
    }

    @Override
    public TeeWriter getTeeWriter()
    {
        return new AvroTeeWriter();
    }

    @Override
    public CachedFileReader getCachedFileReader()
    {
        return new AvroFileReader();
    }

    @Override
    public PostCondition getPostCondition(Configuration conf, JsonNode json, Path path) throws IOException
    {
        Schema avroSchema = AvroUtils.getSchema(conf, path);
        BlockSchema schema = AvroUtils.convertToBlockSchema(avroSchema);
        return new PostCondition(schema, null, null);
    }

}
