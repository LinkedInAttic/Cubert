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

package com.linkedin.cubert.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.operator.PhaseContext;
import com.linkedin.cubert.operator.PostCondition;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.operator.PreconditionExceptionType;
import com.linkedin.cubert.operator.TupleOperator;
import com.linkedin.cubert.utils.CommonUtils;
import com.linkedin.cubert.utils.JsonUtils;

public class ListFiles implements TupleOperator
{
    private Iterator<String> iterator;
    private Tuple output;

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        List<String> files = new ArrayList<String>();
        String dirsStr = JsonUtils.getText(json.get("args"), "dirs");
        String[] dirs = CommonUtils.trim(dirsStr.split(","));

        for (String dir : dirs)
        {
            Path path = new Path(dir);
            FileSystem fs = path.getFileSystem(PhaseContext.getConf());
            FileStatus[] allStatus = fs.globStatus(path);

            if (allStatus == null || allStatus.length == 0)
                continue;

            for (FileStatus status : allStatus)
            {
                if (status.isDir())
                {
                    listFiles(fs, status.getPath(), files);
                }
                else
                {
                    files.add(status.getPath().toUri().getPath());
                }
            }

        }

        iterator = files.iterator();
        output = TupleFactory.getInstance().newTuple(1);
    }

    private void listFiles(FileSystem fs, Path path, List<String> files) throws IOException
    {
        FileStatus[] allStatus = fs.listStatus(path);
        if (allStatus == null)
            return;

        for (FileStatus status : allStatus)
        {
            if (status.isDir())
            {
                listFiles(fs, status.getPath(), files);
            }
            else
            {
                files.add(status.getPath().toUri().getPath());
            }
        }

    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        if (!iterator.hasNext())
            return null;

        output.set(0, iterator.next());
        return output;
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        if (!json.has("args") || !json.get("args").has("dirs"))
        {
            throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG,
                                            "dirs parameter not specified");
        }
        BlockSchema schema = new BlockSchema("STRING filename");
        return new PostCondition(schema, null, null);
    }

}
