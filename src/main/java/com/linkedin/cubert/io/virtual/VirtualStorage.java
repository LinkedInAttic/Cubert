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

package com.linkedin.cubert.io.virtual;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.BlockWriter;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.block.TupleCreator;
import com.linkedin.cubert.io.CachedFileReader;
import com.linkedin.cubert.io.Storage;
import com.linkedin.cubert.io.TeeWriter;
import com.linkedin.cubert.operator.PostCondition;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * Input storage that is not backed by a filesystem. This storage launches mappers (the
 * number of mappers is defined in the "mappers" property for the storage).
 * <p>
 * This input storage will not generate any data.
 * 
 * @author Vinitha Gankidi
 * 
 */
public class VirtualStorage implements Storage
{
    @Override
    public void prepareInput(Job job,
                             Configuration conf,
                             JsonNode params,
                             List<Path> paths) throws IOException
    {
        if (params.has("mappers"))
        {
            conf.set("mappers", JsonUtils.getText(params, "mappers"));
        }
        job.setInputFormatClass(VirtualInputFormat.class);
    }

    @Override
    public TupleCreator getTupleCreator()
    {
        return new TupleCreator()
        {
            @Override
            public Tuple create(Object arg0, Object arg1) throws IOException
            {
                return null;
            }

            @Override
            public void setup(JsonNode arg0) throws IOException
            {

            }

        };
    }

    @Override
    public PostCondition getPostCondition(Configuration conf, JsonNode json, Path path) throws IOException
    {
        ColumnType[] columnTypes = new ColumnType[1];
        columnTypes[0] = new ColumnType("dummy", DataType.LONG);
        BlockSchema schema = new BlockSchema(columnTypes);
        return new PostCondition(schema, null, null);
    }

    @Override
    public void prepareOutput(Job job,
                              Configuration conf,
                              JsonNode params,
                              BlockSchema schema,
                              Path path)
    {
        throw new UnsupportedOperationException("VIRTUAL storage can only used for input");
    }

    @Override
    public BlockWriter getBlockWriter()
    {
        throw new UnsupportedOperationException("VIRTUAL storage can only used for input");
    }

    @Override
    public TeeWriter getTeeWriter()
    {
        throw new UnsupportedOperationException("VIRTUAL storage can only used for input");
    }

    @Override
    public CachedFileReader getCachedFileReader()
    {
        throw new UnsupportedOperationException("VIRTUAL storage can only used reading cached files");
    }

}
