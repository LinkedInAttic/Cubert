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

package com.linkedin.cubert.block;

import java.io.File;
import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.io.CachedFileReader;
import com.linkedin.cubert.io.StorageFactory;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * A block that is backed by data read directly from a file.
 * 
 * 
 * @author Maneesh Varshney
 * 
 */
public class LocalFileBlock implements Block
{
    private final File file;
    private CachedFileReader reader;
    private BlockProperties props;

    public LocalFileBlock(File file)
    {
        this.file = file;
    }

    @Override
    public void configure(JsonNode json) throws IOException
    {
        reader =
                StorageFactory.get(JsonUtils.getText(json, "type")).getCachedFileReader();

        reader.open(json, file);

        props =
                new BlockProperties(JsonUtils.getText(json, "output"),
                                    reader.getSchema(),
                                    (BlockProperties) null);
        props.setBlockId(0);
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        return reader.next();
    }

    @Override
    public void rewind() throws IOException
    {

    }

    @Override
    public BlockProperties getProperties()
    {
        return props;
    }

}
