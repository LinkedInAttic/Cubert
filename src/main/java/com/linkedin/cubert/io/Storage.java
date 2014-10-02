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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.BlockWriter;
import com.linkedin.cubert.block.TupleCreator;
import com.linkedin.cubert.operator.PostCondition;

public interface Storage
{
    void prepareInput(Job job, Configuration conf, JsonNode params, List<Path> paths) throws IOException;

    void prepareOutput(Job job,
                       Configuration conf,
                       JsonNode params,
                       BlockSchema schema,
                       Path path);

    PostCondition getPostCondition(Configuration conf, JsonNode json, Path path) throws IOException;

    TupleCreator getTupleCreator();

    BlockWriter getBlockWriter();

    TeeWriter getTeeWriter();

    CachedFileReader getCachedFileReader();
}
