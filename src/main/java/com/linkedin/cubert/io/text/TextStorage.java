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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.analyzer.physical.PlanRewriteException;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.BlockWriter;
import com.linkedin.cubert.block.TupleCreator;
import com.linkedin.cubert.io.CachedFileReader;
import com.linkedin.cubert.io.Storage;
import com.linkedin.cubert.io.TeeWriter;
import com.linkedin.cubert.operator.PostCondition;
import com.linkedin.cubert.plan.physical.CubertStrings;
import com.linkedin.cubert.utils.JsonUtils;

public class TextStorage implements Storage
{

    @Override
    public void prepareInput(Job job,
                             Configuration conf,
                             JsonNode params,
                             List<Path> paths) throws IOException
    {
        if (params.has("separator"))
        {
            conf.set(CubertStrings.TEXT_OUTPUT_SEPARATOR,
                     JsonUtils.getText(params, "separator"));
        }

        job.setInputFormatClass(PigTextInputFormat.class);
    }

    @Override
    public void prepareOutput(Job job,
                              Configuration conf,
                              JsonNode params,
                              BlockSchema schema,
                              Path path)
    {
        if (params.has("separator"))
        {
            conf.set(CubertStrings.TEXT_OUTPUT_SEPARATOR,
                     JsonUtils.getText(params, "separator"));
        }

        job.setOutputFormatClass(PigTextOutputFormatWrapper.class);
    }

    @Override
    public TupleCreator getTupleCreator()
    {
        return new TextTupleCreator();
    }

    @Override
    public BlockWriter getBlockWriter()
    {
        return new TextBlockWriter();
    }

    @Override
    public TeeWriter getTeeWriter()
    {
        return new TextTeeWriter();
    }

    @Override
    public CachedFileReader getCachedFileReader()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PostCondition getPostCondition(Configuration conf, JsonNode json, Path path) throws IOException
    {
        JsonNode params = json.get("params");
        if (params == null || params.isNull() || !params.has("schema")
                || params.get("schema").isNull())
            throw new PlanRewriteException("Cannot infer schema of TEXT input. Please specify using the 'schema' param.");

        BlockSchema schema =
                new BlockSchema(json.get("params").get("schema").getTextValue());
        return new PostCondition(schema, null, null);
    }

}
