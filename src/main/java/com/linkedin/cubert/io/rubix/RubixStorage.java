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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
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
import com.linkedin.cubert.utils.CommonUtils;
import com.linkedin.cubert.utils.JsonUtils;

public class RubixStorage implements Storage
{

    @Override
    public void prepareInput(Job job,
                             Configuration conf,
                             JsonNode params,
                             List<Path> paths) throws IOException
    {
        job.setInputFormatClass(RubixInputFormat.class);
    }

    @Override
    public void prepareOutput(Job job,
                              Configuration conf,
                              JsonNode params,
                              BlockSchema schema,
                              Path path)
    {
        Class<?> tupleClass = TupleFactory.getInstance().newTuple().getClass();
        job.setOutputKeyClass(tupleClass);
        job.setOutputValueClass(tupleClass);

        job.setOutputFormatClass(RubixOutputFormat.class);

        if (params.has("compact"))
            conf.setBoolean(CubertStrings.USE_COMPACT_SERIALIZATION,
                            Boolean.parseBoolean(JsonUtils.getText(params, "compact")));
    }

    @Override
    public TupleCreator getTupleCreator()
    {
        return new RubixTupleCreator();
    }

    @Override
    public BlockWriter getBlockWriter()
    {
        return new RubixBlockWriter();
    }

    @Override
    public TeeWriter getTeeWriter()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CachedFileReader getCachedFileReader()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PostCondition getPostCondition(Configuration conf, JsonNode json, Path path) throws IOException
    {
        Path afile =
                CommonUtils.getAFileInPath(conf,
                                           path,
                                           RubixConstants.RUBIX_EXTENSION_SUFFIX);
        RubixFile<Tuple, Object> rubixFile = new RubixFile<Tuple, Object>(conf, afile);
        try
        {
            return new PostCondition(rubixFile.getSchema(),
                                     rubixFile.getPartitionKeys(),
                                     rubixFile.getSortKeys());
        }
        catch (ClassNotFoundException e)
        {
            throw new PlanRewriteException(e);
        }

    }

}
