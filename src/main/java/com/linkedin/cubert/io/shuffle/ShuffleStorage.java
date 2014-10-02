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

package com.linkedin.cubert.io.shuffle;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.BlockWriter;
import com.linkedin.cubert.block.MergedTupleCreator;
import com.linkedin.cubert.block.ShuffleBlockWriter;
import com.linkedin.cubert.block.TupleCreator;
import com.linkedin.cubert.io.CachedFileReader;
import com.linkedin.cubert.io.Storage;
import com.linkedin.cubert.io.TeeWriter;
import com.linkedin.cubert.operator.PostCondition;

public class ShuffleStorage implements Storage
{

    @Override
    public void prepareInput(Job job,
                             Configuration conf,
                             JsonNode params,
                             List<Path> paths) throws IOException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void prepareOutput(Job job,
                              Configuration conf,
                              JsonNode params,
                              BlockSchema schema,
                              Path path)
    {
        Class<?> tupleClass = TupleFactory.getInstance().newTuple().getClass();
        job.setMapOutputKeyClass(tupleClass);
        job.setMapOutputValueClass(tupleClass);
    }

    @Override
    public TupleCreator getTupleCreator()
    {
        return new MergedTupleCreator();
    }

    @Override
    public BlockWriter getBlockWriter()
    {
        return new ShuffleBlockWriter();
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
        throw new UnsupportedOperationException();
    }

}
